# @ubiquify/cyclone

Http(s) client for [@ubiquify/relay](https://github.com/ubiquify/relay) .

## Build

```sh
npm run clean
npm install
npm run build
npm run test
```

# Plumbing client

Low level interface to the relay service.

```ts
interface RelayClientPlumbing {
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
```

```ts
const relayClient = relayClientPlumbingFactory({
  baseURL: "https://localhost:3002",
});
```

# Basic client

Coarse interface to the relay service. The relay auto-merges on push.

```ts
interface RelayClientBasic {
  push(versionStoreRoot: Link, versionRoot?: Link): Promise<BasicPushResponse>;
  pull(
    versionStoreId: string,
    localVersionStoreRoot?: Link
  ): Promise<
    | { versionStore: VersionStore; graphStore: GraphStore; graph: Graph }
    | undefined
  >;
}
```

```ts
const relayClient = relayClientBasicFactory(
  {
    chunk,
    chunkSize,
    linkCodec,
    valueCodec,
    blockStore,
  },
  {
    baseURL: "https://localhost:3002",
  }
);
```

## Licenses

Licensed under either [Apache 2.0](http://opensource.org/licenses/MIT) or [MIT](http://opensource.org/licenses/MIT) at your option.
