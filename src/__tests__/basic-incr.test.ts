import {
  BlockStore,
  Graph,
  GraphStore,
  Link,
  LinkCodec,
  MemoryBlockStore,
  PathElemType,
  Prop,
  RequestBuilder,
  ValueCodec,
  VersionStore,
  chunkerFactory,
  graphStoreFactory,
  linkCodecFactory,
  memoryBlockStoreFactory,
  navigateVertices,
  valueCodecFactory,
  versionStoreFactory,
} from "@ubiquify/core";

import { compute_chunks } from "@dstanesc/wasm-chunking-fastcdc-node";
import https from "https";
import {
  GraphRelay,
  LinkResolver,
  createGraphRelay,
  memoryBlockResolverFactory,
} from "@ubiquify/relay";

import {
  BasicPushResponse,
  RelayClientBasic,
  relayClientBasicFactory,
} from "../index";
import { getCertificate, largeArray } from "./testUtil";

const chunkSize = 512;
const { chunk } = chunkerFactory(chunkSize, compute_chunks);
const linkCodec: LinkCodec = linkCodecFactory();
const valueCodec: ValueCodec = valueCodecFactory();

enum ObjectTypes {
  FOLDER = 1,
  FILE = 2,
}
enum RlshpTypes {
  CONTAINS = 1,
}
enum PropTypes {
  META = 1,
  DATA = 2,
}
enum KeyTypes {
  NAME = 1,
  CONTENT = 2,
  FILL = 3,
}

describe("Basic client with incremental configuration tests", () => {
  let relayBlockStore: BlockStore;
  let blockStore: MemoryBlockStore;
  let linkResolver: LinkResolver;
  let server: any;
  let graphRelay: GraphRelay;
  let relayClient: RelayClientBasic;
  let initialBlocks: MemoryBlockStore;
  let initialBlocks2: MemoryBlockStore;
  beforeAll((done) => {
    blockStore = memoryBlockStoreFactory();
    initialBlocks = memoryBlockStoreFactory();
    initialBlocks2 = memoryBlockStoreFactory();
    relayBlockStore = memoryBlockStoreFactory();
    linkResolver = memoryBlockResolverFactory();
    graphRelay = createGraphRelay(relayBlockStore, linkResolver);
    server = graphRelay.startHttps(3000, getCertificate(), done);
    relayClient = relayClientBasicFactory(
      {
        chunk,
        chunkSize,
        linkCodec,
        valueCodec,
        blockStore,
        incremental: true,
      },
      {
        httpsAgent: new https.Agent({
          rejectUnauthorized: false,
        }),
        baseURL: "https://localhost:3000",
      }
    );
  });

  afterAll((done) => {
    graphRelay.stopHttps(done); // Stop the server
  });

  describe("the relay client", () => {
    let versionStoreId: string;
    let originalStoreRoot: Link;
    it("should push initial graph and history", async () => {
      const versionStore: VersionStore = await versionStoreFactory({
        chunk,
        linkCodec,
        valueCodec,
        blockStore,
      });
      const graphStore: GraphStore = graphStoreFactory({
        chunk,
        linkCodec,
        valueCodec,
        blockStore,
      });
      const graph = new Graph(versionStore, graphStore);
      const tx = graph.tx();
      await tx.start();
      const v1 = tx.addVertex(ObjectTypes.FOLDER);
      const v2 = tx.addVertex(ObjectTypes.FOLDER);
      const v3 = tx.addVertex(ObjectTypes.FILE);
      const e1 = await tx.addEdge(v1, v2, RlshpTypes.CONTAINS);
      const e2 = await tx.addEdge(v1, v3, RlshpTypes.CONTAINS);
      await tx.addVertexProp(v1, KeyTypes.NAME, "root-folder", PropTypes.META);
      await tx.addVertexProp(
        v2,
        KeyTypes.NAME,
        "nested-folder",
        PropTypes.META
      );
      await tx.addVertexProp(v3, KeyTypes.NAME, "nested-file", PropTypes.META);
      await tx.addVertexProp(
        v2,
        KeyTypes.CONTENT,
        "hello world from v2",
        PropTypes.DATA
      );
      await tx.addVertexProp(
        v3,
        KeyTypes.CONTENT,
        "hello world from v3",
        PropTypes.DATA
      );

      // force generating blocks
      await tx.addVertexProp(
        v3,
        KeyTypes.FILL,
        largeArray(1024 * 1024, 123),
        PropTypes.DATA
      );

      const { root: original } = await tx.commit({});

      versionStoreId = versionStore.id();

      originalStoreRoot = versionStore.versionStoreRoot();

      const response: BasicPushResponse = await relayClient.push(
        versionStore.versionStoreRoot()
      );

      blockStore.push(initialBlocks);
      blockStore.push(initialBlocks2);
    });

    it("should pull graph and history", async () => {
      const { versionStore, graphStore, graph } = await relayClient.pull(
        versionStoreId,
        originalStoreRoot
      );

      expect(versionStore.id()).toEqual(versionStoreId);

      expect(versionStore.versionStoreRoot().toString()).toEqual(
        originalStoreRoot.toString()
      );

      const vr = await query(graph);

      expect(vr.length).toEqual(2);
      expect(vr[0].value).toEqual("nested-folder");
      expect(vr[1].value).toEqual("nested-file");
    });

    it("should update existing and pushed result should reflect changes", async () => {
      const { versionStore, graphStore, graph } = await relayClient.pull(
        versionStoreId,
        originalStoreRoot
      );
      const tx = graph.tx();
      await tx.start();
      const v10 = await tx.getVertex(0);
      const v11 = tx.addVertex(ObjectTypes.FILE);
      const e11 = await tx.addEdge(v10, v11, RlshpTypes.CONTAINS);
      await tx.addVertexProp(
        v11,
        KeyTypes.NAME,
        "nested-file-user-1",
        PropTypes.META
      );
      await tx.addVertexProp(
        v11,
        KeyTypes.CONTENT,
        "hello world from v11",
        PropTypes.DATA
      );

      // force generating blocks
      await tx.addVertexProp(
        v11,
        KeyTypes.FILL,
        largeArray(1024 * 1024, 987),
        PropTypes.DATA
      );

      const { root: first } = await tx.commit({});

      const response: BasicPushResponse = await relayClient.push(
        versionStore.versionStoreRoot()
      );
      expect(versionStore.id()).toEqual(versionStoreId);

      expect(response.storeRoot.toString()).toEqual(
        versionStore.versionStoreRoot().toString()
      );

      const memoryStoreNew = memoryBlockStoreFactory();
      const relayClientNew = relayClientBasicFactory(
        {
          chunk,
          chunkSize,
          linkCodec,
          valueCodec,
          blockStore: memoryStoreNew,
          incremental: true,
        },
        {
          httpsAgent: new https.Agent({
            rejectUnauthorized: false,
          }),
          baseURL: "https://localhost:3000",
        }
      );

      const { versionStore: versionStore2, graph: graph2 } =
        await relayClientNew.pull(versionStoreId);

      expect(versionStore.versionStoreRoot().toString()).toEqual(
        versionStore2.versionStoreRoot().toString()
      );

      const vr = await query(graph2);

      expect(vr.length).toEqual(3);
      expect(vr[0].value).toEqual("nested-folder");
      expect(vr[1].value).toEqual("nested-file");
      expect(vr[2].value).toEqual("nested-file-user-1");
    });

    it("should pull incrementally when relay holds additional version, when versionStore partially pre-exists on client", async () => {
      // only original version in the block store
      const relayClientInitialBlocks = relayClientBasicFactory(
        {
          chunk,
          chunkSize,
          linkCodec,
          valueCodec,
          blockStore: initialBlocks,
          incremental: true,
        },
        {
          httpsAgent: new https.Agent({
            rejectUnauthorized: false,
          }),
          baseURL: "https://localhost:3000",
        }
      );
      const { versionStore, graph } = await relayClientInitialBlocks.pull(
        versionStoreId,
        originalStoreRoot
      );

      const vr = await query(graph);

      expect(vr.length).toEqual(3);
      expect(vr[0].value).toEqual("nested-folder");
      expect(vr[1].value).toEqual("nested-file");
      expect(vr[2].value).toEqual("nested-file-user-1");
    });

    it("should pull incrementally when relay holds additional version, when versionStore is new", async () => {
      const emptyBlockStore = memoryBlockStoreFactory();
      const relayClientEmpty = relayClientBasicFactory(
        {
          chunk,
          chunkSize,
          linkCodec,
          valueCodec,
          blockStore: emptyBlockStore,
          incremental: true,
        },
        {
          httpsAgent: new https.Agent({
            rejectUnauthorized: false,
          }),
          baseURL: "https://localhost:3000",
        }
      );
      const { versionStore, graph } = await relayClientEmpty.pull(
        versionStoreId
      );

      const vr = await query(graph);

      expect(vr.length).toEqual(3);
      expect(vr[0].value).toEqual("nested-folder");
      expect(vr[1].value).toEqual("nested-file");
      expect(vr[2].value).toEqual("nested-file-user-1");
    });

    it("should pull non-incremental when relay holds additional version", async () => {
      // only original version in the block store
      const relayClientInitialBlocks = relayClientBasicFactory(
        {
          chunk,
          chunkSize,
          linkCodec,
          valueCodec,
          blockStore: initialBlocks2,
        },
        {
          httpsAgent: new https.Agent({
            rejectUnauthorized: false,
          }),
          baseURL: "https://localhost:3000",
        }
      );
      const { versionStore, graph } = await relayClientInitialBlocks.pull(
        versionStoreId,
        originalStoreRoot
      );

      const vr = await query(graph);

      expect(vr.length).toEqual(3);
      expect(vr[0].value).toEqual("nested-folder");
      expect(vr[1].value).toEqual("nested-file");
      expect(vr[2].value).toEqual("nested-file-user-1");
    });
  });
});

const query = async (graph: Graph): Promise<Prop[]> => {
  const request = new RequestBuilder()
    .add(PathElemType.VERTEX)
    .add(PathElemType.EDGE)
    .add(PathElemType.VERTEX)
    .extract(KeyTypes.NAME)
    .maxResults(100)
    .get();

  const vr: Prop[] = [];
  for await (const result of navigateVertices(graph, [0], request)) {
    vr.push(result as Prop);
  }
  return vr;
};
