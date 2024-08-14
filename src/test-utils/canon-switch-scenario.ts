import { Tags } from "@actyx/sdk";
import { MakeCType, WFBusinessOrMarker } from "../consts.js";
import { Enum } from "../utils.js";
import { setup, SetupSystemParams } from "./scenario-builder.js";
import { Code, WFWorkflow } from "../wfcode.js";

const EvNames = Enum([
  "start",
  "L1Bid",
  "L1Accept",
  "L1FirstAdvertise",
  "L1TriggerLoop",
  "L1SecondAdvertise",
  "L1Finalize",
  "L1Compensate",
] as const);
type EvNames = Enum<typeof EvNames>;

const Role = Enum(["canonizer", "worker"] as const);
type Role = Enum<typeof Role>;

type TheType = MakeCType<{ ev: EvNames; role: Role }>;

const workflowTag = Tags<WFBusinessOrMarker<TheType>>("workflowtag");
const code = Code.make<TheType>();
const { role, unique } = code.actor;

const logistic: WFWorkflow<TheType> = {
  uniqueParams: [],
  code: [
    code.event(role(Role.canonizer), EvNames.start, {
      bindings: [
        code.bind("canonizer1", "canonizer1"),
        code.bind("canonizer2", "canonizer2"),
      ],
    }),
    ...code.parallel({ min: 1 }, [
      code.event(role(Role.worker), EvNames.L1Bid, {
        bindings: [code.bind("bidder", "bidder")],
      }),
    ] as const),
    code.event(role(Role.worker), EvNames.L1Accept, {
      bindings: [code.bind("l1", "assignee")],
    }),
    ...code.compensate(
      [
        ...code.retry([
          code.event(unique("l1"), EvNames.L1FirstAdvertise),
          code.canonize(unique("canonizer1")),
          ...code.choice([
            code.event(unique("l1"), EvNames.L1SecondAdvertise),
            code.event(unique("l1"), EvNames.L1TriggerLoop, {
              control: code.Control.fail,
            }),
          ]),
          code.canonize(unique("canonizer2")),
        ]),
        code.event(unique("l1"), EvNames.L1Finalize),
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
      { id: "authoritative1", role: Role.canonizer },
      { id: "authoritative2", role: Role.canonizer },
      { id: "t1", role: Role.worker },
      { id: "t2", role: Role.worker },
      { id: "t3", role: Role.worker },
    ]
  );

  const [authoritative1, authoritative2, t1, t2, t3] = scenario.agents;

  return {
    ...scenario,
    agents: { authoritative1, authoritative2, t1, t2, t3 } as const,
  };
};

export namespace CanonSwitch {
  export const genScenario = genScenarioImpl;
  export type CType = TheType;
  export type Ev = EvNames;
  export const Ev = EvNames;
  export type Scenario = ChoicesScenario;
}
