import { Tags } from "@actyx/sdk";
import { MakeCType, WFBusinessOrMarker } from "../consts.js";
import { Enum } from "../utils.js";
import { setup, SetupSystemParams } from "./scenario-builder.js";
import { Code, WFWorkflow } from "../wfcode.js";

const EvNames = Enum([
  "start",
  "L1Bid",
  "L1Accept",
  "L1Compensate",
  "L2Bid",
  "L2Accept",
  "end",
  "L2Compensate",
] as const);
type EvNames = Enum<typeof EvNames>;

const Role = Enum(["requester", "responder"] as const);
type Role = Enum<typeof Role>;

type TheType = MakeCType<{ ev: EvNames; role: Role }>;

const workflowTag = Tags<WFBusinessOrMarker<TheType>>("workflowtag");
const code = Code.make<TheType>();
const { role, unique } = code.actor;

const logistic: WFWorkflow<TheType> = {
  uniqueParams: [],
  code: [
    code.event(role(Role.requester), EvNames.start),
    ...code.parallel({ min: 1 }, [
      code.event(role(Role.responder), EvNames.L1Bid, {
        bindings: [code.bind("bidder", "bidder")],
      }),
    ] as const),
    code.event(role(Role.responder), EvNames.L1Accept, {
      bindings: [code.bind("l1", "assignee")],
    }),
    ...code.compensate(
      [
        ...code.parallel({ min: 1 }, [
          code.event(role(Role.responder), EvNames.L2Bid, {
            bindings: [code.bind("l2", "bidder")],
          }),
        ]),
        code.event(role(Role.responder), EvNames.L2Accept, {
          bindings: [code.bind("l2", "assignee")],
        }),
        ...code.compensate(
          [code.event(unique("l2"), EvNames.end)],
          [code.event(unique("l2"), EvNames.L2Compensate)]
        ),
      ],
      [code.event(unique("l1"), EvNames.L1Compensate)]
    ),
  ] as const,
};

type ChoicesScenario = ReturnType<typeof genScenarioImpl>;

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
      { id: "client", role: Role.requester },
      { id: "t1", role: Role.responder },
      { id: "t2", role: Role.responder },
      { id: "t3", role: Role.responder },
    ]
  );

  const [client, t1, t2, t3] = scenario.agents;

  return {
    ...scenario,
    agents: { client, t1, t2, t3 } as const,
  };
};

export namespace Choices {
  export const genScenario = genScenarioImpl;
  export type CType = TheType;
  export type Ev = EvNames;
  export const Ev = EvNames;
  export type Scenario = ChoicesScenario;
}
