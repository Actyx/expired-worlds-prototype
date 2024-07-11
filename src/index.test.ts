import { describe, expect, it } from "@jest/globals";
import { run } from "./index.js";
import { Network, Node } from "./ax-mock/index.js";
import {
  ActyxWFBusinessOrMarker,
  MakeCType,
  WFBusinessOrMarker,
} from "./consts.js";
import { Enum } from "./utils.js";
import { ActyxEvent, Tags } from "@actyx/sdk";
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
          code.event(unique("t"), Ev.atSrc),
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

/**
 * Scenario Setup Parameters
 */
type SetupSystemParams = {
  /**
   * The initial data that should be loaded by the nodes at initialization
   */
  initialStoreData?: ActyxEvent<WFBusinessOrMarker<TheType>>[];
};

const setup = (
  params: { id: string; role: Role }[],
  networkParams?: SetupSystemParams
) => {
  const makenetwork = Network.make<WFBusinessOrMarker<TheType>>;
  const makenode = Node.make<WFBusinessOrMarker<TheType>>;

  const network = makenetwork();

  const agents = params
    .map((identity) => {
      // initialize nodes and load all initial data
      const node = makenode({ id: identity.id });
      node.logger.sub(log);
      if (networkParams?.initialStoreData) {
        node.store.load(networkParams.initialStoreData);
      }
      return { identity, node };
    })
    .map((x) => {
      // join network to node
      network.join(x.node);
      return x;
    })
    .map(({ identity, node }) => {
      // run machine
      const machine = run(
        { self: identity, tags: workflowTag },
        node,
        logistic
      );
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

  const findAndRunCommand = async (
    agent: Agent,
    name: string,
    payload?: Record<string, unknown>
  ) => {
    await awhile();
    const command = findCommand(agent, name);
    command.publish(payload);
    await awhile();
  };

  return {
    agents,
    findAgent,
    findCommand,
    findAndRunCommand,
    network,
  };
};

type Scenario = ReturnType<typeof genScenario>;
const genScenario = (setupSystemParams?: SetupSystemParams) => {
  const scenario = setup(
    [
      { id: "storage-src", role: Role.storage },
      { id: "storage-dst", role: Role.storage },
      { id: "manager", role: Role.manager },
      { id: "t1", role: Role.transporter },
      { id: "t2", role: Role.transporter },
      { id: "t3", role: Role.transporter },
    ],
    setupSystemParams
  );

  const [src, dst, manager, t1, t2, t3] = scenario.agents;

  return {
    ...scenario,
    agents: { src, dst, manager, t1, t2, t3 } as const,
  };
};
const expectAllToHaveSameState = (
  agents: Scenario["agents"][keyof Scenario["agents"]][]
) => {
  const first = agents.at(0);
  if (!first) return;
  const rest = agents.slice(1);
  const firstState = first.machine.machine().state();
  rest.forEach((rest) => {
    const restState = rest.machine.machine().state();
    expect(firstState).toEqual(restState);
  });
};

const log = (...args: any[]) =>
  args.forEach((a, i) => {
    process.stdout.write(String(a));
    if (i < args.length - 1) {
      process.stdout.write(" ");
    } else {
      process.stdout.write("\n");
    }
  });

describe("no-partitions", () => {
  it("works", async () => {
    const scenario = genScenario();
    const { findAndRunCommand } = scenario;
    const { dst, manager, src, t1, t2, t3 } = scenario.agents;

    await findAndRunCommand(manager, Ev.request, {
      from: src.identity.id,
      to: dst.identity.id,
      manager: manager.identity.id,
    });

    expectAllToHaveSameState([manager, src, t1, t2, t3]);

    // assert state at request
    expect(dst.machine.machine().state().state?.[0]).toBe(One);
    expect(dst.machine.machine().state().state?.[1].payload.t).toBe("request");

    await findAndRunCommand(t1, Ev.bid, { bidder: t1.identity.id });
    await findAndRunCommand(t2, Ev.bid, { bidder: t2.identity.id });
    await findAndRunCommand(t3, Ev.bid, { bidder: t3.identity.id });

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

    // t1 self assign and accept
    const winner = (() => {
      const state = t1.machine.state().state;
      const winner =
        state &&
        state[0] === Parallel &&
        state[2].find((x) => x.payload.payload.bidder === t1.identity.id)
          ?.payload.payload.bidder;
      if (!winner) throw new Error("no winner");
      return winner;
    })();
    await findAndRunCommand(t1, Ev.assign, { robotID: winner });
    await findAndRunCommand(t1, Ev.accept);

    expect(dst.machine.machine().state().state?.[0]).toBe(One);
    expect(dst.machine.machine().state().state?.[1].payload.t).toBe(Ev.accept);

    // docking t1 -> src and loading
    await findAndRunCommand(t1, Ev.atSrc);
    await findAndRunCommand(t1, Ev.reqEnter);
    await findAndRunCommand(src, Ev.doEnter);
    await findAndRunCommand(t1, Ev.inside);

    expect(dst.machine.machine().state().state?.[0]).toBe(One);
    expect(dst.machine.machine().state().state?.[1].payload.t).toBe(Ev.inside);

    await findAndRunCommand(t1, Ev.reqLeave);
    await findAndRunCommand(src, Ev.doLeave);
    await findAndRunCommand(t1, Ev.success);

    // load
    await findAndRunCommand(t1, Ev.loaded);

    // docking t1 -> dst and loading
    await findAndRunCommand(t1, Ev.atDst);
    await findAndRunCommand(t1, Ev.reqEnter);
    await findAndRunCommand(dst, Ev.doEnter);
    await findAndRunCommand(t1, Ev.inside);
    await findAndRunCommand(t1, Ev.reqLeave);
    await findAndRunCommand(dst, Ev.doLeave);
    await findAndRunCommand(t1, Ev.success);

    // unload
    await findAndRunCommand(t1, Ev.unloaded);

    // done
    await findAndRunCommand(manager, Ev.done);

    expectAllToHaveSameState([manager, src, t1, t2, t3]);
  });
});

describe("partitions-multi-level compensations", () => {
  /**
   * Partitions src and t2 alone
   */
  const scenarioContestingBidOnPartition = async ({
    findAndRunCommand,
    network,
    agents: { dst, manager, src, t1, t2, t3 },
  }: Scenario) => {
    await findAndRunCommand(manager, Ev.request, {
      from: src.identity.id,
      to: dst.identity.id,
      manager: manager.identity.id,
    });

    // partitions
    network.partitions.make([t2.node, src.node]);

    // both t1 and t2 assign and accepts
    // both will have the same lamport timestamp because of the partition,
    // but t1 will win because of stream id sort
    await findAndRunCommand(t1, Ev.bid, { bidder: t1.identity.id });
    await findAndRunCommand(t1, Ev.assign, { robotID: t1.identity.id });
    await findAndRunCommand(t1, Ev.accept);

    await findAndRunCommand(t2, Ev.bid, { bidder: t2.identity.id });
    await findAndRunCommand(t2, Ev.assign, { robotID: t2.identity.id });
    await findAndRunCommand(t2, Ev.accept);
  };

  it("converge correctly after partition is closed", async () => {
    const scenario = genScenario();
    const {
      findAndRunCommand,
      network,
      agents: { dst, manager, src, t1, t2, t3 },
    } = scenario;

    await scenarioContestingBidOnPartition(scenario);

    // expect each of t1 and t2 to have their own "reality"
    expect(t1.machine.machine().state().state?.[1].payload.t).toBe(Ev.accept);
    expect(t1.machine.machine().state().context.t).toBe("t1");

    expect(t2.machine.machine().state().state?.[1].payload.t).toBe(Ev.accept);
    expect(t2.machine.machine().state().context.t).toBe("t2");

    expectAllToHaveSameState([manager, t1, t3]);
    expectAllToHaveSameState([src, t2]);

    // after partition, expect same state for all machines
    await network.partitions.clear();
    await awhile();
  });

  it("does compensation correctly", async () => {
    const scenario = genScenario();
    const {
      findAndRunCommand,
      network,
      agents: { dst, manager, src, t1, t2, t3 },
    } = scenario;

    // isolate t2 and src together
    // t2 and src believes that they should be working together
    await scenarioContestingBidOnPartition(scenario);

    await findAndRunCommand(t2, Ev.atSrc);
    await findAndRunCommand(t2, Ev.reqEnter);
    await findAndRunCommand(src, Ev.doEnter);

    expect(t1.machine.machine().state().state?.[1].payload.t).toBe(Ev.accept);
    expect(t1.machine.machine().state().context.t).toBe("t1");

    expect(t2.machine.machine().state().state?.[1].payload.t).toBe(Ev.doEnter);
    expect(t2.machine.machine().state().context.A).toBe("t2");
    expectAllToHaveSameState([t1, manager, dst]);
    expectAllToHaveSameState([t2, src]);

    network.logger.sub(log);
    src.node.logger.sub(log);
    t2.node.logger.sub(log);
    src.machine.logger.sub(log);
    t2.machine.logger.sub(log);

    await network.partitions.clear();

    expect(t2.machine.mcomb().t).toBe("compensating");
    expect(src.machine.mcomb().t).toBe("compensating");

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(dst.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    // compensating
    await findAndRunCommand(t2, Ev.withdrawn);

    // compensation is done
    expect(t2.machine.mcomb().t).toBe("normal");
    expect(src.machine.mcomb().t).toBe("normal");

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(dst.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    expectAllToHaveSameState([t1, t2, src, manager, dst, t3]);
  });

  // prettier-ignore
  const comp_history = ([{"meta":{"offset":0,"appId":"","eventId":"6dc3039e-2894-4914-ae4a-9d8688c9ff6d","isLocalEvent":true,"lamport":0,"stream":"manager","tags":["workflowtag"],"timestampMicros":1720698809630000},"payload":{"t":"request","payload":{"from":"storage-src","to":"storage-dst","manager":"manager"}}},{"meta":{"offset":0,"appId":"","eventId":"0b05a57a-ecc5-42dc-90a5-da74206c5935","isLocalEvent":true,"lamport":6,"stream":"storage-src","tags":["workflowtag","ax:wf:predecessor:07528c25-bfef-4a17-8861-1d3cbe0a1bdf"],"timestampMicros":1720698809635000},"payload":{"t":"doEnter","payload":{}}},{"meta":{"offset":1,"appId":"","eventId":"181b0e65-642a-436b-b542-d1e7911deea3","isLocalEvent":true,"lamport":7,"stream":"storage-src","tags":["workflowtag"],"timestampMicros":1720698809649000},"payload":{"ax":"ax:wf:compensation:needed:","actor":"storage-src","fromTimelineOf":"07528c25-bfef-4a17-8861-1d3cbe0a1bdf","toTimelineOf":"9967f952-db85-493c-b5f0-2ee1fcfe2989","codeIndex":[13,1]}},{"meta":{"offset":0,"appId":"","eventId":"349059c3-62f0-475d-bd61-010caf680b0c","isLocalEvent":true,"lamport":1,"stream":"t1","tags":["workflowtag","ax:wf:predecessor:6dc3039e-2894-4914-ae4a-9d8688c9ff6d"],"timestampMicros":1720698809631000},"payload":{"t":"bid","payload":{"bidder":"t1"}}},{"meta":{"offset":1,"appId":"","eventId":"07126407-1274-4b3c-8087-1f7ee57c0821","isLocalEvent":true,"lamport":2,"stream":"t1","tags":["workflowtag","ax:wf:predecessor:6dc3039e-2894-4914-ae4a-9d8688c9ff6d"],"timestampMicros":1720698809632000},"payload":{"t":"assign","payload":{"robotID":"t1"}}},{"meta":{"offset":2,"appId":"","eventId":"9967f952-db85-493c-b5f0-2ee1fcfe2989","isLocalEvent":true,"lamport":3,"stream":"t1","tags":["workflowtag","ax:wf:predecessor:07126407-1274-4b3c-8087-1f7ee57c0821"],"timestampMicros":1720698809632000},"payload":{"t":"accept","payload":{}}},{"meta":{"offset":0,"appId":"","eventId":"b183f4b8-8a66-455b-ac0c-688dd52b5d78","isLocalEvent":true,"lamport":1,"stream":"t2","tags":["workflowtag","ax:wf:predecessor:6dc3039e-2894-4914-ae4a-9d8688c9ff6d"],"timestampMicros":1720698809633000},"payload":{"t":"bid","payload":{"bidder":"t2"}}},{"meta":{"offset":1,"appId":"","eventId":"802bd3c7-36c1-4d52-9179-936f8fbdcdee","isLocalEvent":true,"lamport":2,"stream":"t2","tags":["workflowtag","ax:wf:predecessor:6dc3039e-2894-4914-ae4a-9d8688c9ff6d"],"timestampMicros":1720698809633000},"payload":{"t":"assign","payload":{"robotID":"t2"}}},{"meta":{"offset":2,"appId":"","eventId":"2586ee84-1c57-4866-9c9c-225915a62adf","isLocalEvent":true,"lamport":3,"stream":"t2","tags":["workflowtag","ax:wf:predecessor:802bd3c7-36c1-4d52-9179-936f8fbdcdee"],"timestampMicros":1720698809633000},"payload":{"t":"accept","payload":{}}},{"meta":{"offset":3,"appId":"","eventId":"f82d80ba-cecb-4f52-be7e-48fce4404872","isLocalEvent":true,"lamport":4,"stream":"t2","tags":["workflowtag","ax:wf:predecessor:2586ee84-1c57-4866-9c9c-225915a62adf"],"timestampMicros":1720698809634000},"payload":{"t":"atSrc","payload":{}}},{"meta":{"offset":4,"appId":"","eventId":"07528c25-bfef-4a17-8861-1d3cbe0a1bdf","isLocalEvent":true,"lamport":5,"stream":"t2","tags":["workflowtag","ax:wf:predecessor:f82d80ba-cecb-4f52-be7e-48fce4404872"],"timestampMicros":1720698809634000},"payload":{"t":"reqEnter","payload":{}}},{"meta":{"offset":5,"appId":"","eventId":"b7d342ef-cedf-44b5-a601-72aa168d50f5","isLocalEvent":true,"lamport":8,"stream":"t2","tags":["workflowtag"],"timestampMicros":1720698809652000},"payload":{"ax":"ax:wf:compensation:needed:","actor":"t2","fromTimelineOf":"07528c25-bfef-4a17-8861-1d3cbe0a1bdf","toTimelineOf":"9967f952-db85-493c-b5f0-2ee1fcfe2989","codeIndex":[13,1]}}])
    .map((entry) => {
      const date = new Date(Math.round(entry.meta.timestampMicros / 1000))
      return ({
        meta: ({
          ...entry.meta,
          timestampAsDate: () => date,
        }),
        payload: entry.payload
      }) as ActyxWFBusinessOrMarker<TheType>
    })

  it("remembers compensation", async () => {
    // history is loaded that triggers compensation in t2 and src (the above
    // scenario before Ev.withdrawn is called)
    const scenario = genScenario({
      initialStoreData: comp_history,
    });
    const {
      findAndRunCommand,
      network,
      agents: { dst, manager, src, t1, t2, t3 },
    } = scenario;

    await awhile();

    // t2 and src is in doEnter
    [t2, src].forEach((m) =>
      expect(m.machine.machine().state()?.state?.[1].payload.t).toBe(Ev.doEnter)
    );

    [t1, manager, dst, t3].forEach((m) =>
      expect(m.machine.machine().state()?.state?.[1].payload.t).toBe(Ev.accept)
    );

    expect(t2.machine.mcomb().t).toBe("compensating");
    expect(src.machine.mcomb().t).toBe("compensating");

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(dst.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    // t2 compensating, src follows
    await findAndRunCommand(t2, Ev.withdrawn);

    // compensation is done
    expect(t2.machine.mcomb().t).toBe("normal");
    expect(src.machine.mcomb().t).toBe("normal");

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(dst.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    expectAllToHaveSameState([t1, t2, src, manager, dst, t3]);
  });
});
