import { describe, expect, it } from "@jest/globals";
import { Node, Network, StreamStore } from "./index.js";
import * as uuid from "uuid";

const streamOf = (node: Node.Type) => node.api.stores().own.slice();
const accordingToStreamOf = (observer: Node.Type, node: Node.Type) =>
  observer.api.stores().remote.get(node.id)?.slice() || [];

describe("ax-mock", () => {
  const prepare = async () => {
    const network = Network.make();
    const nodes = [
      Node.make({ id: "A" }),
      Node.make({ id: "B" }),
      Node.make({ id: "C" }),
      Node.make({ id: "D" }),
    ] as const;
    await Promise.all(nodes.map((node) => network.join(node)));
    return { network, nodes };
  };

  it("should propagate events", async () => {
    const {
      nodes: [nodeA, nodeB, nodeC, nodeD],
    } = await prepare();

    nodeA.api.publish("a");
    nodeA.api.publish("b");

    expect(nodeA.api.offsetMap()[nodeA.id]).toBe(2);
    expect(nodeB.api.offsetMap()[nodeA.id]).toBe(2);
    expect(nodeC.api.offsetMap()[nodeA.id]).toBe(2);
    expect(nodeD.api.offsetMap()[nodeA.id]).toBe(2);
  });

  it("should propagate events", async () => {
    const {
      network,
      nodes: [nodeA, nodeB, nodeC, nodeD],
    } = await prepare();

    nodeA.api.publish("a");

    network.partitions.make([nodeC, nodeD]);

    nodeA.api.publish("a");

    expect(nodeA.api.offsetMap()[nodeA.id]).toBe(2);
    expect(nodeB.api.offsetMap()[nodeA.id]).toBe(2);
    expect(nodeC.api.offsetMap()[nodeA.id]).toBe(1);
    expect(nodeD.api.offsetMap()[nodeA.id]).toBe(1);

    expect(streamOf(nodeA)).toEqual(accordingToStreamOf(nodeB, nodeA));
    expect(streamOf(nodeB)).toEqual(accordingToStreamOf(nodeA, nodeB));
    expect(streamOf(nodeC)).toEqual(accordingToStreamOf(nodeD, nodeC));
    expect(streamOf(nodeD)).toEqual(accordingToStreamOf(nodeC, nodeD));

    nodeC.api.publish("a");

    expect(nodeA.api.offsetMap()[nodeC.id]).toBe(undefined);
    expect(nodeB.api.offsetMap()[nodeC.id]).toBe(undefined);
    expect(nodeC.api.offsetMap()[nodeC.id]).toBe(1);
    expect(nodeD.api.offsetMap()[nodeC.id]).toBe(1);

    expect(streamOf(nodeA)).toEqual(accordingToStreamOf(nodeB, nodeA));
    expect(streamOf(nodeB)).toEqual(accordingToStreamOf(nodeA, nodeB));
    expect(streamOf(nodeC)).toEqual(accordingToStreamOf(nodeD, nodeC));
    expect(streamOf(nodeD)).toEqual(accordingToStreamOf(nodeC, nodeD));

    await network.partitions.clear();

    expect(nodeA.api.offsetMap()).toEqual(nodeB.api.offsetMap());
    expect(nodeA.api.offsetMap()).toEqual(nodeC.api.offsetMap());
    expect(nodeA.api.offsetMap()).toEqual(nodeD.api.offsetMap());

    expect(streamOf(nodeA)).toEqual(accordingToStreamOf(nodeB, nodeA));
    expect(streamOf(nodeA)).toEqual(accordingToStreamOf(nodeC, nodeA));
    expect(streamOf(nodeA)).toEqual(accordingToStreamOf(nodeD, nodeA));
    expect(streamOf(nodeC)).toEqual(accordingToStreamOf(nodeA, nodeC));
    expect(streamOf(nodeC)).toEqual(accordingToStreamOf(nodeB, nodeC));
    expect(streamOf(nodeC)).toEqual(accordingToStreamOf(nodeD, nodeC));
  });
});
