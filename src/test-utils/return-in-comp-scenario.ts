import { Tags } from "@actyx/sdk";
import { MakeCType, WFBusinessOrMarker } from "../consts.js";
import { Enum } from "../utils.js";
import { setup, SetupSystemParams } from "./scenario-builder.js";
import { Code, WFWorkflow } from "../wfcode.js";

const EvNames = Enum([
  "start",
  "assign",
  "L1Bid",
  "L1Accept",
  "L1FirstAdvertise",
  "L1Finalize",
  "L1CompensateManual",
  "L1Compensate1",
  "L1Compensate2Ret",
  "L1Compensate2Normal",
  "L1Compensate3Ret",
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
    code.event(role(Role.canonizer), EvNames.start),
    code.event(role(Role.canonizer), EvNames.assign, {
      bindings: [code.bind("canonizer", "canonizer")],
    }),
    code.event(role(Role.worker), EvNames.L1Accept, {
      bindings: [code.bind("a", "a"), code.bind("b", "b")],
    }),
    ...code.compensate(
      [
        code.event(unique("a"), EvNames.L1FirstAdvertise),
        code.canonize(unique("canonizer")),
        code.event(unique("b"), EvNames.L1Finalize),
      ],
      [
        code.event(unique("a"), EvNames.L1Compensate1),
        ...code.timeout(
          0,
          [
            code.event(unique("a"), EvNames.L1Compensate1),
            ...code.choice([
              code.event(unique("b"), EvNames.L1Compensate2Ret, {
                control: code.Control.return,
              }),
              code.event(unique("a"), EvNames.L1Compensate2Normal),
            ]),
            code.event(unique("b"), EvNames.L1Compensate3Ret),
          ],
          // TODO: canonizer here will likely cause a deadlock because the
          // canonizer is not necessarily in the partition that causes the
          // compensateable
          code.event(unique("canonizer"), EvNames.L1CompensateManual, {
            control: code.Control.return,
          })
        ),
      ]
    ),
  ] as const,
};

type RetInCompScenario = ReturnType<typeof genScenarioImpl>;

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
      { id: "authoritative3", role: Role.canonizer },
      { id: "t1", role: Role.worker },
      { id: "t2", role: Role.worker },
      { id: "t3", role: Role.worker },
      { id: "t4", role: Role.worker },
    ]
  );

  const [authoritative1, authoritative2, authoritative3, t1, t2, t3, t4] =
    scenario.agents;

  return {
    ...scenario,
    agents: {
      authoritative1,
      authoritative2,
      authoritative3,
      t1,
      t2,
      t3,
      t4,
    } as const,
  };
};

export namespace ReturnInComp {
  export const genScenario = genScenarioImpl;
  export type CType = TheType;
  export type Ev = EvNames;
  export const Ev = EvNames;
  export type Scenario = RetInCompScenario;
}
