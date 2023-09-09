import { Block, BlockStore } from "@ubiquify/core";

export interface MapBlockSink extends BlockStore {
  getBlocks(): Map<string, Block>;
}

export const memoryBlockSinkFactory = (): MapBlockSink => {
  const blocks = new Map<string, Block>();
  return {
    getBlocks: () => blocks,
    put: async (block: { cid: any; bytes: Uint8Array }) => {
      blocks.set(block.cid.toString(), block);
    },
    get: async (cid: any) => {
      const block = blocks.get(cid.toString());
      if (block !== undefined) {
        return block.bytes;
      } else {
        throw new Error("Block Not found for " + cid.toString());
      }
    },
  };
};
