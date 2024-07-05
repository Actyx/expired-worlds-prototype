import { describe, expect, it } from "@jest/globals";
import { run } from "./index.js";
import { Network, Node } from "./ax-mock/index.js";
import { MakeCType, WFBusinessOrMarker } from "./consts.js";
import { Enum } from "./utils.js";
import { Tags } from "@actyx/sdk";
import { Code, Exact, Otherwise, WFWorkflow } from "./wfcode.js";
import { One, Parallel } from "./wfmachine.js";

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

const setup = (params: { id: string; role: Role }[]) => {
  const makenetwork = Network.make<WFBusinessOrMarker<TheType>>;
  const makenode = Node.make<WFBusinessOrMarker<TheType>>;

  const network = makenetwork();

  const agents = params.map((identity) => {
    const node = makenode({ id: identity.id });
    const machine = run({ self: identity, tags: workflowTag }, node, logistic);
    network.join(node);
    return { identity, node, machine };
  });
  type Agent = (typeof agents)[any];

  const findAgent = (fn: (c: Agent) => boolean): Agent => {
    const agent = agents.find(fn);
    if (!agent) throw new Error("findAgent error");
    return agent;
  };

  const findCommand = (agent: Agent, name: string) => {
    const found = agent.machine
      .commands()
      .find(({ info }) => info.name === name);
    if (!found)
      throw new Error(`command ${name} not found in ${agent.identity.id}`);
    return found;
  };

  return {
    agents,
    findAgent,
    network,
    findCommand,
  };
};

const genScenario = () => {
  const scenario = setup([
    { id: "storage-src", role: Role.storage },
    { id: "storage-dst", role: Role.storage },
    { id: "manager", role: Role.manager },
    { id: "t1", role: Role.transporter },
    { id: "t2", role: Role.transporter },
    { id: "t3", role: Role.transporter },
  ]);
  const [src, dst, manager, t1, t2, t3] = scenario.agents;

  return {
    ...scenario,
    agents: { src, dst, manager, t1, t2, t3 } as const,
  };
};
type Scenario = ReturnType<typeof genScenario>;

/**
 * request
 */
const sequenceOne = async (scenario: Scenario) => {
  const { findCommand, agents } = scenario;
  const { dst, manager, src } = agents;
  await awhile();
  findCommand(manager, Ev.request).publish({
    from: src.identity.id,
    to: dst.identity.id,
    manager: manager.identity.id,
  });
  await awhile();
};

/**
 * bidding
 */
const sequenceBidding = async ({
  findCommand,
  agents: { t1, t2, t3 },
}: Scenario) => {
  await awhile();

  findCommand(t1, Ev.bid).publish({ bidder: t1.identity.id });
  findCommand(t2, Ev.bid).publish({ bidder: t2.identity.id });
  findCommand(t3, Ev.bid).publish({ bidder: t3.identity.id });
  await awhile();
};

const sequenceSelfAssignAndAcceptByT1 = async ({
  findCommand,
  agents: { t1 },
}: Scenario) => {
  const state = t1.machine.state().state;
  const winner =
    state &&
    state[0] === Parallel &&
    state[2].find((x) => x.payload.payload.bidder === t1.identity.id)?.payload
      .payload.bidder;
  if (!winner) throw new Error("no winner");

  findCommand(t1, Ev.assign).publish({ robotID: winner });
  await awhile();
  findCommand(t1, Ev.accept).publish();
  await awhile();
};

const sequenceGoToDst = async ({ findCommand, agents: { t1 } }: Scenario) => {
  findCommand(t1, Ev.atDst).publish();
  await awhile();
};

const sequenceT1SrcDockingFirstPart = async ({
  findCommand,
  agents: { t1, src },
}: Scenario) => {
  findCommand(t1, Ev.reqEnter).publish();
  await awhile();
  findCommand(src, Ev.doEnter).publish();
  await awhile();
  findCommand(t1, Ev.inside).publish();
  await awhile();
};

const sequenceT1SrcDockingSecondPart = async ({
  findCommand,
  agents: { t1, src },
}: Scenario) => {
  findCommand(t1, Ev.reqLeave).publish();
  await awhile();
  findCommand(src, Ev.doLeave).publish();
  await awhile();
  findCommand(t1, Ev.success).publish();
  await awhile();
};

const sequenceSelfAssignByT2 = async ({
  findCommand,
  agents: { t2 },
}: Scenario) => {
  const state = t2.machine.state().state;
  const winner =
    state &&
    state[0] === Parallel &&
    state[2].find((x) => x.payload.payload.bidder === t2.identity.id);

  findCommand(t2, Ev.assign).publish({
    robotID: winner,
  });

  await awhile();
};

describe("single", () => {
  it("works", async () => {
    const scenario = genScenario();
    await sequenceOne(scenario);
    const { dst, manager, src, t1, t2, t3 } = scenario.agents;

    // assert all machine has the same state
    [manager, src, t1, t2, t3].forEach((x) => {
      expect(dst.machine.machine().state()).toEqual(
        x.machine.machine().state()
      );
    });

    // assert state at request
    expect(dst.machine.machine().state().state?.[0]).toBe(One);
    expect(dst.machine.machine().state().state?.[1].payload.t).toBe("request");

    await sequenceBidding(scenario);

    // assert base state at request
    // and there are 3 bids in parallel
    expect(dst.machine.machine().state().state?.[0]).toBe(Parallel);
    expect(dst.machine.machine().state().state?.[1].payload.t).toBe("request");
    expect(dst.machine.machine().state().state?.[2]?.length).toBe(3);
    expect(
      dst.machine
        .machine()
        .state()
        .state?.[2]?.find((x) => x.payload.payload.bidder === t1.identity.id)
    ).toBeTruthy();
    expect(
      dst.machine
        .machine()
        .state()
        .state?.[2]?.find((x) => x.payload.payload.bidder === t2.identity.id)
    ).toBeTruthy();
    expect(
      dst.machine
        .machine()
        .state()
        .state?.[2]?.find((x) => x.payload.payload.bidder === t3.identity.id)
    ).toBeTruthy();

    sequenceSelfAssignAndAcceptByT1(scenario);
    // sequenceSelfAssignByT2(scenario);

    await awhile();
    expect(dst.machine.machine().state().state?.[0]).toBe(One);
    expect(dst.machine.machine().state().state?.[1].payload.t).toBe(Ev.accept);

    await sequenceGoToDst(scenario);
    await sequenceT1SrcDockingFirstPart(scenario);
    expect(dst.machine.machine().state().state?.[0]).toBe(One);
    expect(dst.machine.machine().state().state?.[1].payload.t).toBe(Ev.inside);
    await sequenceT1SrcDockingSecondPart(scenario);
  });
});
