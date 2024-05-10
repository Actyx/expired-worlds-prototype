import { describe, expect, it } from "@jest/globals";
import { Reality, run } from "./index.js";
import { v4 as uuidv4 } from "uuid";
import { Network, Node } from "./ax-mock/index.js";
import { CTypeProto, MakeCType, WFBusinessOrMarker } from "./consts.js";
import { Code, Exact, Otherwise, WFWorkflow } from "./wfmachine.js";
import { Enum } from "./utils.js";
import { Tags } from "@actyx/sdk";

const Ev = Enum([
  "request",
  "bid",
  "cancelled",
  "assign",
  "accept",
  "notAccepted",
  "atSrc",
  "reqEnter",
  "doEnter",
  "deny",
  "inside",
  "withdrawn",
  "reqLeave",
  "doLeave",
  "success",
  "withdraw",
  "doLeave",
  "withdrawn",
  "loaded",
  "notPickedUp",
  "atDst",
  "notDelivered",
  "unloaded",
  "reqStorage",
  "offerStorage",
  "acceptStorage",
  "assistanceNeeded",
  "atWarehouse",
  "stashed",
  "assistanceNeeded",
  "logisticFailed",
  "done",
] as const);
type Ev = Enum<typeof Ev>;

const Role = Enum(["manager", "transporter", "storage"] as const);
type Role = Enum<typeof Role>;

type TheType = MakeCType<{
  ev: Ev;
  role: Role;
}>;

const workflowTag = Tags<WFBusinessOrMarker<TheType>>("workflowtag");
const code = Code.make<TheType>();
const { role, unique } = code.actor;

const docking: WFWorkflow<TheType> = {
  uniqueParams: ["A", "B"],
  code: [
    code.event(unique("A"), Ev.reqEnter),
    ...code.compensate(
      [
        ...code.choice([
          code.event(unique("B"), Ev.doEnter),
          code.event(unique("B"), Ev.deny, { control: "return" }),
        ]),
        code.event(unique("A"), Ev.inside),
      ],
      [code.event(unique("A"), Ev.withdrawn)]
    ),
    ...code.compensate(
      [
        code.event(unique("A"), Ev.reqLeave),
        code.event(unique("B"), Ev.doLeave),
        code.event(unique("A"), Ev.success, { control: "return" }),
      ],
      [
        code.event(unique("A"), Ev.withdraw),
        code.event(unique("B"), Ev.doLeave),
        code.event(unique("A"), Ev.withdrawn),
      ]
    ),
  ],
};

const logistic: WFWorkflow<TheType> = {
  uniqueParams: [],
  code: [
    code.event(role(Role.manager), Ev.request, {
      bindings: [
        code.bind("src", "from"),
        code.bind("dst", "to"),
        code.bind("m", "manager"),
      ],
    }),
    ...code.retry([
      ...code.timeout(
        5 * 60 * 1000,
        [
          ...code.parallel({ min: 1 }, [
            code.event(role(Role.transporter), Ev.bid, {
              bindings: [code.bind("bidder", "bidder")],
            }),
          ] as const),
        ] as const,
        code.event(unique("m"), Ev.cancelled, {
          control: Code.Control.return,
        })
      ),
      ...code.timeout(
        10 * 1000,
        [
          code.event(role(Role.transporter), Ev.assign, {
            bindings: [code.bind("t", "robotID")],
          }),
          code.event(unique("t"), Ev.accept),
        ] as const,
        code.event(unique("m"), Ev.notAccepted, {
          control: Code.Control.fail,
        })
      ),
      ...code.timeout(
        30 * 60 * 1000,
        [
          code.event(unique("t"), Ev.atDst),
          ...code.match(docking, { A: "t", B: "src" }, [
            code.matchCase([Exact, Ev.success], [
              code.event(unique("t"), Ev.loaded),
            ] as const),
            code.matchCase([Otherwise], [
              code.event(unique("t"), Ev.notPickedUp, {
                control: "fail",
              }),
            ] as const),
          ]),
          ...code.compensate(
            [
              code.event(unique("t"), Ev.atDst),
              ...code.match(docking, { A: "t", B: "dst" }, [
                code.matchCase(
                  [Exact, Ev.success],
                  [code.event(unique("t"), Ev.unloaded)]
                ),
                code.matchCase(
                  [Otherwise],
                  [
                    code.event(unique("t"), Ev.notDelivered, {
                      control: "fail",
                    }),
                  ]
                ),
              ]),
            ],
            [
              code.event(unique("t"), Ev.reqStorage),
              ...code.timeout(
                10 * 1000,
                [
                  code.event(role(Role.storage), Ev.offerStorage, {
                    bindings: [code.bind("s", "storage")],
                  }),
                ],
                code.event(unique("t"), Ev.assistanceNeeded, {
                  control: "return",
                })
              ),
              code.event(unique("t"), Ev.atWarehouse),
              ...code.match(docking, { A: "t", B: "s" }, [
                code.matchCase(
                  [Exact, Ev.success],
                  [code.event(unique("t"), Ev.stashed)]
                ),
                code.matchCase(
                  [Otherwise],
                  [
                    code.event(unique("t"), Ev.assistanceNeeded, {
                      control: "return",
                    }),
                  ]
                ),
              ]),
            ]
          ),
        ] as const,
        code.event(unique("m"), Ev.logisticFailed, {
          control: Code.Control.return,
        })
      ),
    ] as const),
    code.event(unique("m"), Ev.done),
  ] as const,
};

/**
 * promise that waits for other timers to resolve
 */
const awhile = () => new Promise(setImmediate);

describe("x", () => {
  it("x", async () => {
    const makenetwork = Network.make<WFBusinessOrMarker<TheType>>;
    const makenode = Node.make<WFBusinessOrMarker<TheType>>;
    // setup network
    const network = makenetwork();

    const agents = [
      { id: "storage-src", role: Role.storage },
      { id: "storage-dst", role: Role.storage },
      { id: "manager", role: Role.manager },
      { id: "t1", role: Role.transporter },
      { id: "t2", role: Role.transporter },
      { id: "t3", role: Role.transporter },
    ].map((identity) => {
      const { id } = identity;
      const node = makenode({ id });
      const machine = run(
        { self: identity, tags: workflowTag },
        node,
        logistic
      );
      network.join(node);
      return { identity, node, machine };
    });

    await awhile();

    const findCommand = (agent: (typeof agents)[0], name: string) => {
      const found = agent.machine.commands().find((x) => x.info.name);
      if (!found) throw new Error("command not found");
      return found;
    };

    const [src, dst, manager, t1, t2, t3] = agents;

    findCommand(manager, Ev.request).publish({
      src: src.identity.id,
      dst: dst.identity.id,
      m: manager.identity.id,
    });

    findCommand(t1, Ev.bid).publish({ bidder: t1.identity.id });
    findCommand(t2, Ev.bid).publish({ bidder: t2.identity.id });
    findCommand(t3, Ev.bid).publish({ bidder: t3.identity.id });

    console.log(manager.machine.machine().state());
    console.log(t1.machine.machine().state());
    console.log(manager.machine.machine().availableCommands());

    findCommand(manager, Ev.assign).publish({ t: t1.identity.id });
  });
});

// describe("witness", () => {
//   it("should track if reality is canon", async () => {
//     const witness = Reality.witness<EventType>({ id: identify });

//     witness.see(event2);

//     const reality = await witness.canonReality();

//     expect(reality.isCanon()).toBe(true);
//     expect(reality.isExpired()).toBe(false);

//     witness.retrospect();

//     expect(reality.isCanon()).toBe(false);
//     expect(reality.isExpired()).toBe(true);
//   });

//   it("should work", async () => {
//     const witness = Reality.witness<EventType>({ id: identify });

//     witness.see(event2);
//     witness.see(event3);
//     witness.see(event5);

//     const reality1 = await witness.canonReality();

//     witness.retrospect();

//     witness.see(event1);
//     witness.see(event2);
//     witness.see(event3);
//     witness.see(event4);
//     witness.see(event5);

//     const amendments = reality1.amendments();
//     expect(amendments).not.toBe(Reality.AmendmentsNotReady);
//     if (amendments === Reality.AmendmentsNotReady) throw new Error("fail");

//     const first = amendments.at(0);
//     expect(first).toBeTruthy();
//     if (!first) throw new Error("fail");

//     expect(first.past.history()).toEqual([event2, event3, event5]);
//     expect(first.future.history()).toEqual([
//       event1,
//       event2,
//       event3,
//       event4,
//       event5,
//     ]);
//     expect(first.divergentPoint).toEqual(event2);

//     const reality2 = await witness.canonReality();
//     expect(reality1.history() === first.past.history());
//     expect(reality2.history() === first.future.history());
//   });

//   it("should track latest data", async () => {
//     const witness = Reality.witness<EventType>({ id: identify });

//     witness.see(event1);
//     const reality = await witness.canonReality();

//     expect(reality.latest()).toBe(event1);
//     witness.see(event2);
//     expect(reality.latest()).toBe(event2);
//     witness.see(event3);
//     expect(reality.latest()).toBe(event3);
//     witness.see(event4);
//     expect(reality.latest()).toBe(event4);
//     witness.see(event5);
//     expect(reality.latest()).toBe(event5);
//   });

//   it("should track previous snapshots", async () => {
//     const witness = Reality.witness<EventType>({ id: identify });

//     witness.see(event1);
//     witness.see(event2);
//     witness.see(event3);
//     witness.see(event4);
//     witness.see(event5);

//     const reality = await witness.canonReality();
//     const snap4 = reality.previous();
//     expect(snap4?.latest()).toBe(event4);
//     const snap3 = snap4?.previous();
//     expect(snap3?.latest()).toBe(event3);
//     const snap2 = snap3?.previous();
//     expect(snap2?.latest()).toBe(event2);
//     const snap1 = snap2?.previous();
//     expect(snap1?.latest()).toBe(event1);
//     const snapnull = snap1?.previous();
//     expect(snapnull).toBe(null);
//   });

//   it("should track root", async () => {
//     const witness = Reality.witness<EventType>({ id: identify });

//     witness.see(event1);
//     witness.see(event2);
//     witness.see(event3);
//     witness.see(event4);
//     witness.see(event5);

//     const reality = await witness.canonReality();
//     expect(reality.first().latest()).toBe(event1);
//   });

//   it("should be able to get reality from snapshot", async () => {
//     const witness = Reality.witness<EventType>({ id: identify });

//     witness.see(event1);
//     witness.see(event2);
//     witness.see(event3);
//     witness.see(event4);
//     witness.see(event5);

//     const reality = await witness.canonReality();
//     expect(reality.first().real()).toBe(reality);
//   });
// });
