import { Tags } from "@actyx/sdk";
import { MakeCType, WFBusinessOrMarker } from "../consts.js";
import { Enum } from "../utils.js";
import { setup, SetupSystemParams } from "./scenario-builder.js";
import { Code, Exact, Otherwise, WFWorkflow } from "../wfcode.js";

const Evs = Enum([
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
type Evs = Enum<typeof Evs>;

const Role = Enum(["manager", "transporter", "storage"] as const);
type Role = Enum<typeof Role>;

type TheType = MakeCType<{ ev: Evs; role: Role }>;

const workflowTag = Tags<WFBusinessOrMarker<TheType>>("workflowtag");
const code = Code.make<TheType>();
const { role, unique } = code.actor;

const docking: WFWorkflow<TheType> = {
  uniqueParams: ["A", "B"],
  code: [
    code.event(unique("A"), Evs.reqEnter),
    ...code.compensate(
      [
        ...code.choice([
          code.event(unique("B"), Evs.doEnter),
          code.event(unique("B"), Evs.deny, {
            control: code.Control.return,
          }),
        ]),
        code.event(unique("A"), Evs.inside),
      ],
      [code.event(unique("A"), Evs.withdrawn)]
    ),
    ...code.compensate(
      [
        code.event(unique("A"), Evs.reqLeave),
        code.event(unique("B"), Evs.doLeave),
        code.event(unique("A"), Evs.success, {
          control: code.Control.return,
        }),
      ],
      [
        code.event(unique("A"), Evs.withdraw),
        code.event(unique("B"), Evs.doLeave),
        code.event(unique("A"), Evs.withdrawn),
      ]
    ),
  ],
};

const logistic: WFWorkflow<TheType> = {
  uniqueParams: [],
  code: [
    code.event(role(Role.manager), Evs.request, {
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
            code.event(role(Role.transporter), Evs.bid, {
              bindings: [code.bind("bidder", "bidder")],
            }),
          ] as const),
        ] as const,
        code.event(unique("m"), Evs.cancelled, {
          control: Code.Control.return,
        })
      ),
      ...code.timeout(
        10 * 1000,
        [
          code.event(role(Role.transporter), Evs.assign, {
            bindings: [code.bind("t", "robotID")],
          }),
          code.event(unique("t"), Evs.accept),
        ] as const,
        code.event(unique("m"), Evs.notAccepted, {
          control: Code.Control.fail,
        })
      ),
      ...code.timeout(
        30 * 60 * 1000,
        [
          code.event(unique("t"), Evs.atSrc),
          ...code.match(docking, { A: "t", B: "src" }, [
            code.matchCase([Exact, Evs.success], [
              code.event(unique("t"), Evs.loaded),
            ] as const),
            code.matchCase([Otherwise], [
              code.event(unique("t"), Evs.notPickedUp, {
                control: code.Control.fail,
              }),
            ] as const),
          ]),
          ...code.compensate(
            [
              code.event(unique("t"), Evs.atDst),
              ...code.match(docking, { A: "t", B: "dst" }, [
                code.matchCase(
                  [Exact, Evs.success],
                  [code.event(unique("t"), Evs.unloaded)]
                ),
                code.matchCase(
                  [Otherwise],
                  [
                    code.event(unique("t"), Evs.notDelivered, {
                      control: code.Control.fail,
                    }),
                  ]
                ),
              ]),
            ],
            [
              code.event(unique("t"), Evs.reqStorage),
              ...code.timeout(
                10 * 1000,
                [
                  code.event(unique("src"), Evs.offerStorage, {
                    bindings: [code.bind("s", "storage")],
                  }),
                ],
                code.event(unique("t"), Evs.assistanceNeeded, {
                  control: code.Control.return,
                })
              ),
              code.event(unique("t"), Evs.atWarehouse),
              ...code.match(docking, { A: "t", B: "src" }, [
                code.matchCase(
                  [Exact, Evs.success],
                  [code.event(unique("t"), Evs.stashed)]
                ),
                code.matchCase(
                  [Otherwise],
                  [
                    code.event(unique("t"), Evs.assistanceNeeded, {
                      control: code.Control.return,
                    }),
                  ]
                ),
              ]),
            ]
          ),
        ] as const,
        code.event(unique("m"), Evs.logisticFailed, {
          control: Code.Control.return,
        })
      ),
    ] as const),
    code.event(unique("m"), Evs.done),
  ] as const,
};

type LogisticScenario = ReturnType<typeof genScenarioImpl>;

const genScenarioImpl = (
  setupSystemParams?: Partial<SetupSystemParams<TheType>>
) => {
  const scenario = setup<TheType>(
    logistic,
    {
      ...(setupSystemParams || {}),
      tags: workflowTag,
    },
    [
      { id: "storage-src", role: Role.storage },
      { id: "storage-dst", role: Role.storage },
      { id: "manager", role: Role.manager },
      { id: "t1", role: Role.transporter },
      { id: "t2", role: Role.transporter },
      { id: "t3", role: Role.transporter },
    ]
  );

  const [src, dst, manager, t1, t2, t3] = scenario.agents;

  return {
    ...scenario,
    agents: { src, dst, manager, t1, t2, t3 } as const,
  };
};

export namespace Logistics {
  export const genScenario = genScenarioImpl;
  export type CType = TheType;
  export type Ev = Evs;
  export const Ev = Evs;
  export type Scenario = LogisticScenario;
}
