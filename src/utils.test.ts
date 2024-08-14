import { describe, expect, it } from "@jest/globals";
import { MultihashMap } from "./utils.js";

describe("MultiHashMap", () => {
  it("works", () => {
    const multihashMap = MultihashMap.depth(3).make<
      [string, number, boolean],
      symbol
    >();

    const value = Symbol();

    expect(multihashMap.has(["", 0, false])).toBe(false);
    multihashMap.set(["", 0, false], value);
    expect(multihashMap.has(["", 0, false])).toBe(true);
    expect(multihashMap.get(["", 0, false])).toBe(value);
  });
});
