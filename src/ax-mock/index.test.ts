import { describe, expect, it } from "@jest/globals";
import { Node, Network, XEventKey, XLamport, Inner } from "./index.js";
import { ActyxEvent, EventKey, Lamport, MsgType, Tag, Tags } from "@actyx/sdk";
import { afterEach } from "node:test";
import { Ord, sleep } from "../utils.js";

const streamOf = <E>(node: Node.Type<E>) => node.api.stores().own.slice();
const accordingToStreamOf = <E>(observer: Node.Type<E>, node: Node.Type<E>) =>
  observer.api.stores().remote.get(node.id)?.slice() || [];

const DefaultTags = Tag<string>("default");
const tag = (p: string) => DefaultTags.applyTyped(p);

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

    nodeA.api.publish(tag("a"));
    nodeA.api.publish(tag("a"));

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

    nodeA.api.publish(tag("a"));

    network.partitions.make([nodeC, nodeD]);

    nodeA.api.publish(tag("a"));

    expect(nodeA.api.offsetMap()[nodeA.id]).toBe(2);
    expect(nodeB.api.offsetMap()[nodeA.id]).toBe(2);
    expect(nodeC.api.offsetMap()[nodeA.id]).toBe(1);
    expect(nodeD.api.offsetMap()[nodeA.id]).toBe(1);

    expect(streamOf(nodeA)).toEqual(accordingToStreamOf(nodeB, nodeA));
    expect(streamOf(nodeB)).toEqual(accordingToStreamOf(nodeA, nodeB));
    expect(streamOf(nodeC)).toEqual(accordingToStreamOf(nodeD, nodeC));
    expect(streamOf(nodeD)).toEqual(accordingToStreamOf(nodeC, nodeD));

    nodeC.api.publish(tag("a"));

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

  describe("subscription", () => {
    type Payload = string;
    let unsubs = [] as Function[];
    afterEach(() => {
      unsubs.forEach((x) => x());
      unsubs = [];
    });
    it("should work", async () => {
      const network = Network.make<Payload>();
      const [nodeA, nodeB] = [
        Node.make<Payload>({ id: "A" }),
        Node.make<Payload>({ id: "B" }),
      ];
      await network.join(nodeA);
      await network.join(nodeB);

      let observedEvents: ActyxEvent<Payload>[] = [];
      nodeA.api.publish(tag("a1"));
      nodeB.api.publish(tag("b1"));

      unsubs.push(
        nodeA.api.subscribeMonotonic((e) => {
          if (e.type === MsgType.events) {
            observedEvents.push(...e.events);
          }
          if (e.type === MsgType.timetravel) {
            observedEvents = [];
          }
        })
      );

      await sleep(3);
      expect(observedEvents.at(0)?.payload).toEqual("a1");
      expect(observedEvents.at(1)?.payload).toEqual("b1");

      network.partitions.make([nodeB]);
      await sleep(3);
      nodeA.api.publish(tag("a2"));
      nodeB.api.publish(tag("b2"));
      nodeA.api.publish(tag("a3"));
      nodeB.api.publish(tag("b3"));
      expect(observedEvents.at(2)?.payload).toEqual("a2");
      expect(observedEvents.at(3)?.payload).toEqual("a3");

      await network.partitions.clear();
      await sleep(3);

      expect(observedEvents.map((x) => x.payload)).toEqual([
        "a1",
        "b1",
        "a2",
        "b2",
        "a3",
        "b3",
      ]);
      await sleep(3);
    });
  });
});

describe("event-key", () => {
  it("is orderable", () => {
    const eventkeys: XEventKey.Type[] = [
      XEventKey.make(XLamport.make(0), "a"),
      XEventKey.make(XLamport.make(0), "b"),
      XEventKey.make(XLamport.make(1), "b"),
    ].sort((a, b) => Ord.toNum(Ord.cmp(a, b)));

    expect(JSON.stringify(eventkeys)).toEqual(
      JSON.stringify([
        XEventKey.make(XLamport.make(0), "a"),
        XEventKey.make(XLamport.make(0), "b"),
        XEventKey.make(XLamport.make(1), "b"),
      ])
    );
  });
});
