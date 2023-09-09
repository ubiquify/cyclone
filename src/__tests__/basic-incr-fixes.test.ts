import {
  BlockStore,
  Graph,
  GraphStore,
  ItemList,
  ItemValue,
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
  itemListFactory,
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

const chunkSize = 1024 * 24;
const { chunk } = chunkerFactory(chunkSize, compute_chunks);
const linkCodec: LinkCodec = linkCodecFactory();
const valueCodec: ValueCodec = valueCodecFactory();

describe("Basic client with incremental configuration tests", () => {
  let relayBlockStore: BlockStore;
  let linkResolver: LinkResolver;
  let server: any;
  let graphRelay: GraphRelay;
  beforeAll((done) => {
    relayBlockStore = memoryBlockStoreFactory();
    linkResolver = memoryBlockResolverFactory();
    graphRelay = createGraphRelay(relayBlockStore, linkResolver);
    server = graphRelay.startHttps(3000, getCertificate(), done);
  });

  afterAll((done) => {
    graphRelay.stopHttps(done); // Stop the server
  });

  test("one time push multiple times revised, one time pull", async () => {
    const blockStore: BlockStore = memoryBlockStoreFactory();
    const versionStore: VersionStore = await versionStoreFactory({
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
    enum KeyTypes {
      ID = 11,
      NAME = 22,
      CONTENT = 33,
    }
    const itemList: ItemList = itemListFactory(versionStore, graphStore);
    const tx = itemList.tx();
    await tx.start();
    for (let i = 0; i < 100; i++) {
      const itemValue: ItemValue = new Map<number, any>();
      itemValue.set(KeyTypes.ID, i);
      itemValue.set(KeyTypes.NAME, `item ${i}`);
      itemValue.set(KeyTypes.CONTENT, largeArray(1024 * 256, i));
      await tx.push(itemValue);
    }
    // first version
    const { root, index, blocks } = await tx.commit({
      comment: "First commit",
      tags: ["v0.0.1"],
    });

    const tx2 = itemList.tx();
    await tx2.start();
    for (let i = 100; i < 200; i++) {
      const itemValue: ItemValue = new Map<number, any>();
      itemValue.set(KeyTypes.ID, i);
      itemValue.set(KeyTypes.NAME, `item ${i}`);
      itemValue.set(KeyTypes.CONTENT, largeArray(1024 * 128, i * 13));
      await tx2.push(itemValue);
    }
    // second version
    const {
      root: root2,
      index: index2,
      blocks: blocks2,
    } = await tx2.commit({
      comment: "Second commit",
      tags: ["v0.0.2"],
    });

    const tx3 = itemList.tx();
    await tx3.start();
    for (let i = 200; i < 300; i++) {
      const itemValue: ItemValue = new Map<number, any>();
      itemValue.set(KeyTypes.ID, i);
      itemValue.set(KeyTypes.NAME, `item ${i}`);
      itemValue.set(KeyTypes.CONTENT, largeArray(1024 * 128, i * 17));
      await tx3.push(itemValue);
    }
    // third version
    const {
      root: root3,
      index: index3,
      blocks: blocks3,
    } = await tx3.commit({
      comment: "Third commit",
      tags: ["v0.0.3"],
    });

    const tx4 = itemList.tx();
    await tx4.start();
    for (let i = 300; i < 400; i++) {
      const itemValue: ItemValue = new Map<number, any>();
      itemValue.set(KeyTypes.ID, i);
      itemValue.set(KeyTypes.NAME, `item ${i}`);
      itemValue.set(KeyTypes.CONTENT, largeArray(1024 * 128, i * 19));
      await tx4.push(itemValue);
    }
    // fourth version
    const {
      root: root4,
      index: index4,
      blocks: blocks4,
    } = await tx4.commit({
      comment: "Fourth commit",
      tags: ["v0.0.4"],
    });

    const relayClient1: RelayClientBasic = relayClientBasicFactory(
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

    const response: BasicPushResponse = await relayClient1.push(
      versionStore.versionStoreRoot()
    );

    expect(response.storeRoot.toString()).toEqual(
      versionStore.versionStoreRoot().toString()
    );

    const emptyBlockStore = memoryBlockStoreFactory();
    const relayClient2: RelayClientBasic = relayClientBasicFactory(
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

    //  Error handling PullGraphVersion request: Error: Block Not found for bafkreicrvkvwus5qglkt3ft6rwr5g77unxxf5nva6rzyoh7vivzl6e5fea
    const { versionStore: versionStore2, graphStore: graphStore2 } =
      await relayClient2.pull(versionStore.id());

    expect(versionStore.versionStoreRoot().toString()).toEqual(
      versionStore2.versionStoreRoot().toString()
    );
    const itemList2: ItemList = itemListFactory(versionStore2, graphStore2);
    expect(await itemList2.length()).toEqual(400);
  }, 1000000);

  test("incremental sparse push, one time pull onex", async () => {
    const blockStore: BlockStore = memoryBlockStoreFactory();
    const versionStore: VersionStore = await versionStoreFactory({
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
    enum KeyTypes {
      ID = 11,
      NAME = 22,
      CONTENT = 33,
    }
    const itemList: ItemList = itemListFactory(versionStore, graphStore);
    const tx = itemList.tx();
    await tx.start();
    for (let i = 0; i < 100; i++) {
      const itemValue: ItemValue = new Map<number, any>();
      itemValue.set(KeyTypes.ID, i);
      itemValue.set(KeyTypes.NAME, `item ${i}`);
      itemValue.set(KeyTypes.CONTENT, largeArray(1024 * 256, i));
      await tx.push(itemValue);
    }
    // first version
    const { root, index, blocks } = await tx.commit({
      comment: "First commit",
      tags: ["v0.0.1"],
    });

    // push first version
    const relayClient1: RelayClientBasic = relayClientBasicFactory(
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

    const response: BasicPushResponse = await relayClient1.push(
      versionStore.versionStoreRoot()
    );

    expect(response.storeRoot.toString()).toEqual(
      versionStore.versionStoreRoot().toString()
    );

    const tx2 = itemList.tx();
    await tx2.start();
    for (let i = 100; i < 200; i++) {
      const itemValue: ItemValue = new Map<number, any>();
      itemValue.set(KeyTypes.ID, i);
      itemValue.set(KeyTypes.NAME, `item ${i}`);
      itemValue.set(KeyTypes.CONTENT, largeArray(1024 * 128, i * 13));
      await tx2.push(itemValue);
    }
    // second version
    const {
      root: root2,
      index: index2,
      blocks: blocks2,
    } = await tx2.commit({
      comment: "Second commit",
      tags: ["v0.0.2"],
    });

    const tx3 = itemList.tx();
    await tx3.start();
    for (let i = 200; i < 300; i++) {
      const itemValue: ItemValue = new Map<number, any>();
      itemValue.set(KeyTypes.ID, i);
      itemValue.set(KeyTypes.NAME, `item ${i}`);
      itemValue.set(KeyTypes.CONTENT, largeArray(1024 * 128, i * 17));
      await tx3.push(itemValue);
    }
    // third version
    const {
      root: root3,
      index: index3,
      blocks: blocks3,
    } = await tx3.commit({
      comment: "Third commit",
      tags: ["v0.0.3"],
    });

    const tx4 = itemList.tx();
    await tx4.start();
    for (let i = 300; i < 400; i++) {
      const itemValue: ItemValue = new Map<number, any>();
      itemValue.set(KeyTypes.ID, i);
      itemValue.set(KeyTypes.NAME, `item ${i}`);
      itemValue.set(KeyTypes.CONTENT, largeArray(1024 * 128, i * 19));
      await tx4.push(itemValue);
    }
    // fourth version
    const {
      root: root4,
      index: index4,
      blocks: blocks4,
    } = await tx4.commit({
      comment: "Fourth commit",
      tags: ["v0.0.4"],
    });

    // push fourth version
    const response2: BasicPushResponse = await relayClient1.push(
      versionStore.versionStoreRoot(),
      root4
    );

    expect(response2.storeRoot.toString()).toEqual(
      versionStore.versionStoreRoot().toString()
    );

    const emptyBlockStore = memoryBlockStoreFactory();
    const relayClient2: RelayClientBasic = relayClientBasicFactory(
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

    const { versionStore: versionStore2, graphStore: graphStore2 } =
      await relayClient2.pull(versionStore.id());

    expect(versionStore.versionStoreRoot().toString()).toEqual(
      versionStore2.versionStoreRoot().toString()
    );
    const itemList2: ItemList = itemListFactory(versionStore2, graphStore2);
    expect(await itemList2.length()).toEqual(400);
  }, 1000000);
});
