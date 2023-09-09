import {
  Block,
  BlockStore,
  ContentDiff,
  Graph,
  GraphStore,
  Link,
  LinkCodec,
  MemoryBlockStore,
  ValueCodec,
  Version,
  VersionStore,
  blockIndexFactory,
  graphPackerFactory,
  graphStoreFactory,
  memoryBlockStoreFactory,
  versionStoreFactory,
} from "@ubiquify/core";
import axios, { AxiosError, AxiosResponse, CreateAxiosDefaults } from "axios";
import { MapBlockSink, memoryBlockSinkFactory } from "./utils";

export interface RelayClientPlumbing {
  storePush(
    chunkSize: number,
    bytes: Uint8Array
  ): Promise<PlumbingStorePushResponse>;
  storePull(
    chunkSize: number,
    versionStoreId: string
  ): Promise<Uint8Array | undefined>;
  storeResolve(versionStoreId: string): Promise<string | undefined>;
  graphPush(bytes: Uint8Array): Promise<PlumbingGraphPushResponse>;
  graphPull(versionRoot: string): Promise<Uint8Array | undefined>;
  indexPull(versionRoot: string): Promise<Uint8Array | undefined>;
  blocksPush(bytes: Uint8Array): Promise<PlumbingBlocksPushResponse>;
  blocksPull(links: string[]): Promise<Uint8Array | undefined>;
  protocolVersion(): Promise<ProtocolVersion>;
}

export type ProtocolVersion = {
  major: number;
  minor: number;
  patch: number;
};

export type PlumbingStorePushResponse = {
  storeRoot: string;
  versionRoot: string;
};

export type PlumbingGraphPushResponse = {
  versionRoot: string;
};

export type PlumbingBlocksPushResponse = {
  blockCount: number;
};

export type BasicPushResponse = {
  storeRoot: Link;
  versionRoot: Link;
};

export interface RelayClientBasic {
  push(versionStoreRoot: Link, versionRoot?: Link): Promise<BasicPushResponse>;
  pull(
    versionStoreId: string,
    localVersionStoreRoot?: Link
  ): Promise<
    | { versionStore: VersionStore; graphStore: GraphStore; graph: Graph }
    | undefined
  >;
}

export const relayClientBasicFactory = (
  {
    chunk,
    chunkSize,
    linkCodec,
    valueCodec,
    blockStore,
    incremental = false,
    maxBatchSizeBytes = 1024 * 1024 * 10,
  }: {
    chunk: (buffer: Uint8Array) => Uint32Array;
    chunkSize: number;
    linkCodec: LinkCodec;
    valueCodec: ValueCodec;
    blockStore: BlockStore;
    incremental?: boolean;
    maxBatchSizeBytes?: number;
  },
  config: CreateAxiosDefaults<any>
): RelayClientBasic => {
  const plumbing = relayClientPlumbingFactory(config);
  const {
    packVersionStore,
    restoreSingleIndex: restoreVersionStore,
    packGraphVersion,
    packRootIndex,
    packRandomBlocks,
    restoreGraphVersion,
    restoreRootIndex,
    restoreRandomBlocks,
  } = graphPackerFactory(linkCodec);

  const push = async (
    versionStoreRoot: Link,
    versionRoot?: Link
  ): Promise<BasicPushResponse> => {
    let localVersionRoot: Link;
    const localVersionStore: VersionStore = await versionStoreFactory({
      storeRoot: versionStoreRoot,
      chunk,
      linkCodec,
      valueCodec,
      blockStore,
    });
    const localVersionStoreBundle: Block = await packVersionStore(
      versionStoreRoot,
      blockStore,
      chunk,
      valueCodec
    );
    if (versionRoot === undefined) {
      localVersionRoot = localVersionStore.currentRoot();
    } else {
      localVersionRoot = versionRoot;
    }
    const graphVersionBundles: Block[] = [];
    let remoteVersionStoreBytes: Uint8Array | undefined;
    if (incremental) {
      try {
        remoteVersionStoreBytes = await plumbing.storePull(
          chunkSize,
          localVersionStore.id()
        );
      } catch (error) {
        if (axios.isAxiosError(error)) {
          const axiosError: AxiosError = error;
          if (axiosError.response?.status !== 404) {
            throw error;
          }
        }
      }
    }
    if (remoteVersionStoreBytes !== undefined) {
      const diffStore: MemoryBlockStore = memoryBlockStoreFactory();
      const { root: remoteVersionStoreRoot } = await restoreVersionStore(
        remoteVersionStoreBytes,
        diffStore
      );
      const remoteVersionStore: VersionStore = await versionStoreFactory({
        storeRoot: remoteVersionStoreRoot,
        chunk,
        linkCodec,
        valueCodec,
        blockStore: diffStore,
      });
      const remoteVersionRoot: Link = remoteVersionStore.currentRoot();
      const remoteVersionRoots: Link[] = remoteVersionStore
        .log()
        .map((version) => version.root);
      if (
        remoteVersionRoots
          .map((root) => linkCodec.encodeString(root))
          .includes(linkCodec.encodeString(localVersionRoot))
      ) {
        return {
          storeRoot: versionStoreRoot,
          versionRoot: localVersionRoot,
        };
      } else {
        const localVersionRoots: Link[] = localVersionStore
          .log()
          .map((version) => version.root);
        localVersionRoots.reverse();
        const localVersionRootsPredecessors: Link[] = [];
        for (const versionRoot of localVersionRoots) {
          localVersionRootsPredecessors.push(versionRoot);
          if (
            linkCodec.encodeString(versionRoot) ===
            linkCodec.encodeString(localVersionRoot)
          ) {
            break;
          }
        }
        const localVersionRootsToPush: Link[] =
          localVersionRootsPredecessors.filter(
            (versionRoot) =>
              !remoteVersionRoots.some(
                (root) =>
                  linkCodec.encodeString(root) ===
                  linkCodec.encodeString(versionRoot)
              )
          );
        localVersionRootsToPush.reverse();
        const remoteRootIndexBytes = await plumbing.indexPull(
          linkCodec.encodeString(remoteVersionRoot)
        );
        const { blocks: remoteRootIndexBlocks } = await restoreRootIndex(
          remoteRootIndexBytes,
          diffStore
        );
        const blocksToPush: Map<string, Block> = new Map<string, Block>();
        for (const selectedVersionRoot of localVersionRootsToPush) {
          const localRootIndexBundle: Block = await packRootIndex(
            selectedVersionRoot,
            blockStore
          );
          const { blocks: localRootIndexBlocks } = await restoreRootIndex(
            localRootIndexBundle.bytes,
            diffStore
          );
          const selectedBlocks: Block[] = [];
          for (const block of localRootIndexBlocks) {
            const linkString = linkCodec.encodeString(block.cid);
            if (
              !remoteRootIndexBlocks
                .map((block) => linkCodec.encodeString(block.cid))
                .includes(linkString)
            ) {
              selectedBlocks.push(block);
            }
          }
          const blockIndexBuilder = blockIndexFactory({
            linkCodec,
            blockStore: diffStore,
          });
          const contentDiff: ContentDiff =
            await blockIndexBuilder.diffRootIndex({
              currentRoot: remoteVersionRoot,
              otherRoot: selectedVersionRoot,
            });
          for (const link of contentDiff.added) {
            const bytes = await blockStore.get(link);
            const block: Block = { cid: link, bytes };
            selectedBlocks.push(block);
          }
          selectedBlocks.forEach((block) => {
            blocksToPush.set(linkCodec.encodeString(block.cid), block);
          });
        }
        const blocksToPushArray: Block[] = Array.from(blocksToPush.values());
        await packRandomBlocksBatchWise(blocksToPushArray, maxBatchSizeBytes);
        const storePushResponse: {
          storeRoot: string;
          versionRoot: string;
        } = await plumbing.storePush(chunkSize, localVersionStoreBundle.bytes);
        return {
          storeRoot: linkCodec.parseString(storePushResponse.storeRoot),
          versionRoot: linkCodec.parseString(storePushResponse.versionRoot),
        };
      }
    } else {
      const versions = localVersionStore.log();
      const blockSink: MapBlockSink = memoryBlockSinkFactory();
      for (const version of versions) {
        const graphVersionBundle: Block = await packGraphVersion(
          version.root,
          blockStore
        );
        await restoreGraphVersion(graphVersionBundle.bytes, blockSink);
        // const graphPushResponse: { versionRoot: string } =
        //   await plumbing.graphPush(graphVersionBundle.bytes);
      }
      const blocksToPushArray: Block[] = Array.from(
        blockSink.getBlocks().values()
      );
      await packRandomBlocksBatchWise(blocksToPushArray, maxBatchSizeBytes);
      const storePushResponse: {
        storeRoot: string;
        versionRoot: string;
      } = await plumbing.storePush(chunkSize, localVersionStoreBundle.bytes);

      return {
        storeRoot: linkCodec.parseString(storePushResponse.storeRoot),
        versionRoot: linkCodec.parseString(storePushResponse.versionRoot),
      };
    }
  };

  const packRandomBlocksBatchWise = async (
    blocksToPushArray: Block[],
    maxBatchSizeBytes: number
  ) => {
    const blocksToPushBatches: Block[][] = [];
    let currentBatchSizeBytes = 0;
    let currentBatch: Block[] = [];
    for (const block of blocksToPushArray) {
      const blockSizeBytes = block.bytes.byteLength;
      if (
        currentBatch.length > 0 &&
        currentBatchSizeBytes + blockSizeBytes >= maxBatchSizeBytes
      ) {
        blocksToPushBatches.push(currentBatch);
        currentBatch = [];
        currentBatchSizeBytes = 0;
      }
      currentBatch.push(block);
      currentBatchSizeBytes += blockSizeBytes;
    }
    if (currentBatch.length > 0) {
      blocksToPushBatches.push(currentBatch);
    }
    for (const blockBatch of blocksToPushBatches) {
      const diffBundle = await packRandomBlocks(blockBatch);
      const blocksPushResponse: { blockCount: number } =
        await plumbing.blocksPush(diffBundle.bytes);
      if (blocksPushResponse.blockCount !== blockBatch.length) {
        throw new Error(
          `Failed to push all blocks pushed: ${blockBatch.length}, confirmed: ${blocksPushResponse.blockCount}`
        );
      }
    }
  };

  const pull = async (
    versionStoreId: string,
    localVersionStoreRoot?: Link
  ): Promise<
    | { versionStore: VersionStore; graphStore: GraphStore; graph: Graph }
    | undefined
  > => {
    let remoteVersionStoreBytes: Uint8Array | undefined;
    if (incremental && localVersionStoreRoot !== undefined) {
      try {
        remoteVersionStoreBytes = await plumbing.storePull(
          chunkSize,
          versionStoreId
        );
      } catch (error) {
        if (axios.isAxiosError(error)) {
          const axiosError: AxiosError = error;
          if (axiosError.response?.status !== 404) {
            throw error;
          }
        }
      }
      if (remoteVersionStoreBytes !== undefined) {
        const diffStore: MemoryBlockStore = memoryBlockStoreFactory();
        const { root: remoteVersionStoreRoot } = await restoreVersionStore(
          remoteVersionStoreBytes,
          diffStore
        );
        const remoteVersionStore: VersionStore = await versionStoreFactory({
          storeRoot: remoteVersionStoreRoot,
          chunk,
          linkCodec,
          valueCodec,
          blockStore: diffStore,
        });
        const remoteVersionRoot: Link = remoteVersionStore.currentRoot();
        const localVersionStore: VersionStore = await versionStoreFactory({
          storeRoot: localVersionStoreRoot,
          chunk,
          linkCodec,
          valueCodec,
          blockStore,
        });
        const localVersionRoot = localVersionStore.currentRoot();
        if (
          linkCodec.encodeString(localVersionRoot) !==
          linkCodec.encodeString(remoteVersionRoot)
        ) {
          const remoteRootIndexBytes = await plumbing.indexPull(
            linkCodec.encodeString(remoteVersionRoot)
          );
          const { blocks: remoteRootIndexBlocks } = await restoreRootIndex(
            remoteRootIndexBytes,
            diffStore
          );
          const localRootIndexBundle: Block = await packRootIndex(
            localVersionRoot,
            blockStore
          );
          const { blocks: localRootIndexBlocks } = await restoreRootIndex(
            localRootIndexBundle.bytes,
            diffStore
          );
          const requiredBlockIdentifiers: string[] = [];
          for (const block of remoteRootIndexBlocks) {
            const linkString = linkCodec.encodeString(block.cid);
            if (
              !localRootIndexBlocks
                .map((block) => linkCodec.encodeString(block.cid))
                .includes(linkString)
            ) {
              requiredBlockIdentifiers.push(linkString);
            }
          }
          const blockIndexBuilder = blockIndexFactory({
            linkCodec,
            blockStore: diffStore,
          });

          const contentDiff: ContentDiff =
            await blockIndexBuilder.diffRootIndex({
              currentRoot: localVersionRoot,
              otherRoot: remoteVersionRoot,
            });
          for (const link of contentDiff.added) {
            requiredBlockIdentifiers.push(linkCodec.encodeString(link));
          }
          const randomBlocksBundle: Uint8Array | undefined =
            await plumbing.blocksPull(requiredBlockIdentifiers);

          if (randomBlocksBundle !== undefined) {
            const selectedBlocks = await restoreRandomBlocks(
              randomBlocksBundle,
              diffStore
            );
            const localVersionStoreBundle: Block = await packVersionStore(
              localVersionStoreRoot,
              blockStore,
              chunk,
              valueCodec
            );
            const { root: storeRootExisting } = await restoreVersionStore(
              localVersionStoreBundle.bytes,
              diffStore
            );
            const graphStoreBundleExisting: Block = await packGraphVersion(
              localVersionRoot,
              blockStore
            );
            const { root: versionRootExisting } = await restoreGraphVersion(
              graphStoreBundleExisting.bytes,
              diffStore
            );
            const versionStoreExisting: VersionStore =
              await versionStoreFactory({
                storeRoot: localVersionStoreRoot,
                versionRoot: localVersionRoot,
                chunk,
                linkCodec,
                valueCodec,
                blockStore: diffStore,
              });
            const {
              root: mergedRoot,
              index: mergedIndex,
              blocks: mergedBlocks,
            } = await versionStoreExisting.mergeVersions(remoteVersionStore);
            await diffStore.push(blockStore);
            const mergedVersionRoot = versionStoreExisting.currentRoot();
            const mergedVersionStoreRoot =
              versionStoreExisting.versionStoreRoot();
            const versionStore: VersionStore = await versionStoreFactory({
              storeRoot: mergedVersionStoreRoot,
              versionRoot: mergedVersionRoot,
              chunk,
              linkCodec,
              valueCodec,
              blockStore,
            });
            const graphStore = graphStoreFactory({
              chunk,
              linkCodec,
              valueCodec,
              blockStore,
            });
            const graph = new Graph(versionStore, graphStore);
            return {
              versionStore,
              graphStore,
              graph,
            };
          } else {
            throw new Error(
              `Failed to pull selected blocks: ${JSON.stringify(
                requiredBlockIdentifiers
              )}`
            );
          }
        } else {
          const versionStore = await versionStoreFactory({
            storeRoot: localVersionStoreRoot,
            versionRoot: localVersionRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
          });
          const graphStore = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
          });
          const graph = new Graph(versionStore, graphStore);
          return {
            versionStore,
            graphStore,
            graph,
          };
        }
      } else {
        return undefined;
      }
    } else {
      try {
        remoteVersionStoreBytes = await plumbing.storePull(
          chunkSize,
          versionStoreId
        );
      } catch (error) {
        if (axios.isAxiosError(error)) {
          const axiosError: AxiosError = error;
          if (axiosError.response?.status !== 404) {
            throw error;
          }
        }
      }
      if (remoteVersionStoreBytes !== undefined) {
        const transientStore: MemoryBlockStore = memoryBlockStoreFactory();
        const {
          root: versionStoreRoot,
          index: versionStoreIndex,
          blocks: versionStoreBlocks,
        } = await restoreVersionStore(remoteVersionStoreBytes, transientStore);
        const versionStoreRemote: VersionStore = await versionStoreFactory({
          storeRoot: versionStoreRoot,
          chunk,
          linkCodec,
          valueCodec,
          blockStore: transientStore,
        });
        const remoteVersions: Version[] = versionStoreRemote.log();
        for (const version of remoteVersions) {
          try {
            await blockStore.get(version.root);
          } catch (e) {
            const graphVersionBytes = await plumbing.graphPull(
              version.root.toString()
            );
            if (graphVersionBytes !== undefined) {
              await restoreGraphVersion(graphVersionBytes, transientStore);
            }
          }
        }
        if (localVersionStoreRoot !== undefined) {
          const localVersionStoreBundle: Block = await packVersionStore(
            localVersionStoreRoot,
            blockStore,
            chunk,
            valueCodec
          );
          const { root: storeRootExisting } = await restoreVersionStore(
            localVersionStoreBundle.bytes,
            transientStore
          );

          const versionStoreLocal: VersionStore = await versionStoreFactory({
            storeRoot: localVersionStoreRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore: transientStore,
          });

          const localVersions: Version[] = versionStoreLocal.log();
          for (const version of localVersions) {
            const localGraphVersionBundle = await packGraphVersion(
              version.root,
              blockStore
            );
            await restoreGraphVersion(
              localGraphVersionBundle.bytes,
              transientStore
            );
          }
          const {
            root: mergedRoot,
            index: mergedIndex,
            blocks: mergedBlocks,
          } = await versionStoreLocal.mergeVersions(versionStoreRemote);
          await transientStore.push(blockStore);
          const mergedVersionRoot = versionStoreLocal.currentRoot();
          const mergedVersionStoreRoot = versionStoreLocal.versionStoreRoot();
          const versionStore: VersionStore = await versionStoreFactory({
            storeRoot: mergedVersionStoreRoot,
            versionRoot: mergedVersionRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
          });
          const graphStore = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
          });
          const graph = new Graph(versionStore, graphStore);
          return {
            versionStore,
            graphStore,
            graph,
          };
        } else {
          await transientStore.push(blockStore);
          const versionStore: VersionStore = await versionStoreFactory({
            storeRoot: versionStoreRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
          });
          const graphStore = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
          });
          const graph = new Graph(versionStore, graphStore);
          return {
            versionStore,
            graphStore,
            graph,
          };
        }
      } else {
        return undefined;
      }
    }
  };

  return { push, pull };
};

export const relayClientPlumbingFactory = (
  config: CreateAxiosDefaults<any>
): RelayClientPlumbing => {
  const httpClient = axios.create(config);
  const storePush = async (
    chunkSize: number,
    bytes: Uint8Array
  ): Promise<PlumbingStorePushResponse> => {
    const response = await httpClient.put("/store/push", bytes.buffer, {
      params: {
        chunkSize: chunkSize,
      },
      headers: {
        "Content-Type": "application/octet-stream",
      },
    });
    return response.data;
  };

  const storePull = async (
    chunkSize: number,
    versionStoreId: string
  ): Promise<Uint8Array | undefined> => {
    const response: AxiosResponse<ArrayBuffer> = await httpClient.get(
      "/store/pull",
      {
        responseType: "arraybuffer",
        params: {
          chunkSize: chunkSize,
          id: versionStoreId,
        },
      }
    );
    if (response.data) {
      const bytes = new Uint8Array(response.data);
      return bytes;
    } else return undefined;
  };

  const storeResolve = async (
    versionStoreId: string
  ): Promise<string | undefined> => {
    const response = await httpClient.get("/store/resolve", {
      params: {
        id: versionStoreId,
      },
    });
    return response.data;
  };

  const graphPush = async (
    bytes: Uint8Array
  ): Promise<PlumbingGraphPushResponse> => {
    const response = await httpClient.put("/graph/version/push", bytes.buffer, {
      headers: {
        "Content-Type": "application/octet-stream",
      },
    });
    return response.data;
  };

  const graphPull = async (
    versionRoot: string
  ): Promise<Uint8Array | undefined> => {
    const response: AxiosResponse<ArrayBuffer> = await httpClient.get(
      "/graph/version/pull",
      {
        responseType: "arraybuffer",
        params: {
          id: versionRoot,
        },
      }
    );
    if (response.data) {
      const bytes = new Uint8Array(response.data);
      return bytes;
    } else return undefined;
  };

  const indexPull = async (
    versionRoot: string
  ): Promise<Uint8Array | undefined> => {
    const response: AxiosResponse<ArrayBuffer> = await httpClient.get(
      "/graph/index/pull",
      {
        responseType: "arraybuffer",
        params: {
          id: versionRoot,
        },
      }
    );
    if (response.data) {
      const bytes = new Uint8Array(response.data);
      return bytes;
    } else return undefined;
  };

  const blocksPush = async (
    bytes: Uint8Array
  ): Promise<PlumbingBlocksPushResponse> => {
    const response = await httpClient.put("/blocks/push", bytes.buffer, {
      headers: {
        "Content-Type": "application/octet-stream",
      },
    });
    return response.data;
  };

  const blocksPull = async (
    links: string[]
  ): Promise<Uint8Array | undefined> => {
    const response: AxiosResponse<ArrayBuffer> = await httpClient.put(
      "/blocks/pull",
      { links },
      {
        responseType: "arraybuffer",
        headers: {
          "Content-Type": "application/json",
        },
      }
    );
    if (response.data) {
      const bytes = new Uint8Array(response.data);
      return bytes;
    } else return undefined;
  };

  const protocolVersion = async (): Promise<ProtocolVersion> => {
    const response = await httpClient.get<ProtocolVersion>(
      "/protocol/version",
      {
        headers: {
          Accept: "application/json",
        },
      }
    );
    return response.data;
  };

  return {
    storePush,
    storePull,
    storeResolve,
    graphPush,
    graphPull,
    indexPull,
    blocksPush,
    blocksPull,
    protocolVersion,
  };
};
