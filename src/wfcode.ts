import { CTypeProto } from "./consts.js";
import { Enum, WrapType } from "./utils.js";

export type CItem<CType extends CTypeProto> =
  | CAnti
  | CEvent<CType>
  | CRetry
  | CTimeout<CType>
  | CParallel
  | CCompensate
  | CCompensateEnd
  | CCompensateWith
  | CMatch<CType>
  | CMatchCase
  | CChoice;
type CAnti =
  | CAntiRetry
  | CAntiTimeout
  | CAntiParallel
  | CAntiCompensate
  | CAntiMatchCase
  | CAntiChoice;

export const Unique = WrapType.blueprint("Unique").build();
export type Unique = WrapType.TypeOf<typeof Unique>;
export const Role = WrapType.blueprint("Role");
export type Role<Ctype extends CTypeProto> = WrapType.TypeOf<
  WrapType.Utils.Refine<typeof Role, Ctype["role"]>
>;

export type Actor<CType extends CTypeProto> = Unique | Role<CType>;

export type CEventBinding = { var: string; index: string };
export type CEvent<CType extends CTypeProto> = {
  t: "event";
  name: CType["ev"];
  actor: Actor<CType>;
  bindings?: CEventBinding[];
  control?: Code.Control;
};
export type CChoice = { t: "choice"; antiIndexOffset: number };
export type CAntiChoice = { t: "anti-choice" };
export type CMatch<CType extends CTypeProto> = {
  t: "match";
  subworkflow: WFWorkflow<CType>;
  args: Record<string, string>;
  casesIndexOffsets: number[];
};
export const Exact: unique symbol = Symbol("Name");
export const Otherwise: unique symbol = Symbol("Otherwise");
export type CMatchCaseType<CType extends CTypeProto> =
  | [typeof Exact, CType["ev"]]
  | [typeof Otherwise];
export type CMatchCase = {
  t: "match-case";
  case: [typeof Exact, string] | [typeof Otherwise];
};
export type CAntiMatchCase = { t: "anti-match-case"; afterIndexOffset: number };
export type CCompensate = {
  t: "compensate";
  withIndexOffset: number;
  antiIndexOffset: number;
};
export type CCompensateEnd = {
  t: "compensate-end";
  baseIndexOffset: number;
  antiIndexOffset: number;
};
export type CCompensateWith = {
  t: "compensate-with";
  baseIndexOffset: number;
  antiIndexOffset: number;
};
export type CAntiCompensate = { t: "anti-compensate"; baseIndexOffset: number };
export type CParallel = {
  t: "par";
  count:
    | {
        max: number;
        min?: number;
      }
    | {
        max?: number;
        min: number;
      }
    | { max: number; min: number };
  pairOffsetIndex: number;
  firstEventIndex: number;
};
export type CRetry = { t: "retry"; pairOffsetIndex: number };
export type CTimeout<CType extends CTypeProto> = {
  t: "timeout";
  duration: number;
  consequence: CEvent<CType>;
  pairOffsetIndex: number;
};

export type CAntiRetry = { t: "anti-retry"; pairOffsetIndex: number };
export type CAntiTimeout = { t: "anti-timeout"; pairOffsetIndex: number };
export type CAntiParallel = { t: "anti-par"; pairOffsetIndex: number };

export namespace Code {
  export const Control = Enum(["fail", "return"] as const);
  export type Control = Enum<typeof Control>;

  export type CodeMaker<CType extends CTypeProto> = {
    actor: ReturnType<typeof actor<CType>>;
    bind: typeof binding;
    event: typeof event<CType>;
    retry: typeof retry<CType>;
    choice: typeof choice<CType>;
    compensate: typeof compensate<CType>;
    matchCase: typeof matchCase<CType>;
    match: typeof match<CType>;
    parallel: typeof parallel<CType>;
    timeout: typeof timeout<CType>;

    Control: typeof Control;
  };

  export const make = <CType extends CTypeProto>(): CodeMaker<CType> => ({
    actor: actor(),
    bind: binding,
    choice,
    compensate,
    event,
    match,
    matchCase,
    parallel,
    retry,
    timeout,
    Control,
  });

  const binding = (name: string, index: string): CEventBinding => ({
    var: name,
    index,
  });

  const actor = <CType extends CTypeProto>() => {
    const CTypeRole = Role.refine<CType["role"]>().build();
    return {
      role: (role: CType["role"]): Actor<CType> => CTypeRole(role),
      unique: (t: string) => Unique(t),
    };
  };

  const event = <CType extends CTypeProto>(
    actor: Actor<CType>,
    name: CType["ev"],
    x?: Pick<CEvent<CType>, "bindings" | "control">
  ): CEvent<CType> => ({ t: "event", actor, name, ...(x || {}) });

  const retry = <CType extends CTypeProto>(
    workflow: CItem<CType>[]
  ): CItem<CType>[] => [
    { t: "retry", pairOffsetIndex: workflow.length + 1 },
    ...workflow,
    { t: "anti-retry", pairOffsetIndex: (workflow.length + 1) * -1 },
  ];

  const choice = <CType extends CTypeProto>(
    events: [CEvent<CType>, CEvent<CType>, ...CEvent<CType>[]]
  ): CItem<CType>[] => [
    { t: "choice", antiIndexOffset: events.length + 1 },
    ...events,
    { t: "anti-choice" },
  ];

  // [0, 1,2,3, 4]
  // 0 - start
  // 123 - main
  // 4 - end
  // 5 - with
  // 678 - compensation
  // 9 - anti

  const compensate = <CType extends CTypeProto>(
    main: CItem<CType>[],
    compensation: [CEvent<CType>, ...CItem<CType>[]]
  ): CItem<CType>[] => {
    const endOffset = main.length + 1;
    const withOffset = endOffset + 1;
    const antiOffset = withOffset + compensation.length + 1;
    return [
      {
        t: "compensate",
        withIndexOffset: withOffset,
        antiIndexOffset: antiOffset,
      },
      ...main,
      {
        t: "compensate-end",
        baseIndexOffset: endOffset * -1,
        antiIndexOffset: 1 + compensate.length + 1,
      },
      {
        t: "compensate-with",
        baseIndexOffset: withOffset * -1,
        antiIndexOffset: 1 + compensation.length,
      },
      ...compensation,
      {
        t: "anti-compensate",
        baseIndexOffset: antiOffset * -1,
      },
    ];
  };

  const matchCase = <CType extends CTypeProto>(
    t: CMatchCaseType<CType>,
    item: readonly CItem<CType>[]
  ): [CMatchCaseType<CType>, readonly CItem<CType>[]] => [t, item];

  const match = <CType extends CTypeProto>(
    workflow: WFWorkflow<CType>,
    args: Record<string, string>,
    cases: [CMatchCaseType<CType>, readonly CItem<CType>[]][]
  ): CItem<CType>[] => {
    let index = 0;
    const inlinedCases: CItem<CType>[] = [];
    const offsets: number[] = [];

    const afterIndexOffset =
      cases.map((x) => x[1].length).reduce((a, b) => a + b, 0) +
      cases.length * 2 +
      1;

    cases.forEach((c) => {
      index += 1;
      inlinedCases.push({ t: "match-case", case: c[0] });
      offsets.push(index);

      index += c[1].length;
      inlinedCases.push(...c[1]);

      index += 1;
      inlinedCases.push({
        t: "anti-match-case",
        afterIndexOffset: afterIndexOffset - index,
      });
    });

    return [
      { t: "match", casesIndexOffsets: offsets, subworkflow: workflow, args },
      ...inlinedCases,
    ];
  };

  const parallel = <CType extends CTypeProto>(
    count: CParallel["count"],
    workflow: CEvent<CType>[]
  ): CItem<CType>[] => [
    {
      t: "par",
      count,
      pairOffsetIndex: workflow.length + 1,
      firstEventIndex: (() => {
        const firstEventIndex = workflow.findIndex((e) => e.t === "event");
        if (firstEventIndex === -1) throw new Error("ev not found");
        return 1 + firstEventIndex;
      })(),
    },
    ...workflow,
    { t: "anti-par", pairOffsetIndex: workflow.length + 1 },
  ];

  const timeout = <CType extends CTypeProto>(
    duration: number,
    workflow: CItem<CType>[],
    consequence: CEvent<CType>
  ): CItem<CType>[] => [
    {
      t: "timeout",
      duration,
      consequence,
      pairOffsetIndex: workflow.length + 1,
    },
    ...workflow,
    { t: "anti-timeout", pairOffsetIndex: (workflow.length + 1) * -1 },
  ];
}

export type WFWorkflow<CType extends CTypeProto> = {
  uniqueParams: string[];
  code: Readonly<[CEvent<CType>, ...CItem<CType>[]]>;
};

export const validate = <CType extends CTypeProto>(
  workflow: WFWorkflow<CType>
) => {
  validateBindings(workflow);
};

export const validateBindings = <CType extends CTypeProto>(
  workflow: WFWorkflow<CType>
) => {
  const availableContext = new Set(workflow.uniqueParams);
  const errors: string[] = [];

  workflow.code.forEach((x, index) => {
    if (x.t === "match") {
      const params = new Set(x.subworkflow.uniqueParams);

      Object.entries(x.args).forEach(([binding, val]) => {
        if (!availableContext.has(val)) {
          errors.push(
            `missing assigned value ${val} from context to ${binding} at index ${index}`
          );
        }

        params.delete(binding);
      });

      if (params.size > 0) {
        errors.push(
          `unassigned match params: ${Array.from(params).join(
            ", "
          )}, at index ${index}`
        );
      }
    }

    if (x.t === "event") {
      if (x.actor.t === "Unique") {
        const uniqueName = x.actor.get();
        if (!availableContext.has(uniqueName)) {
          errors.push(`missing actor binding ${uniqueName} at index ${index}`);
        }
      }

      x.bindings?.forEach((binding) => {
        availableContext.add(binding.var);
      });
    }
  });

  if (errors.length > 0) {
    throw new Error(errors.map((x) => `- ${x.trim()}`).join("\n"));
  }
};

export namespace CParallelIndexer {
  export const make = <CType extends CTypeProto>(
    workflow: WFWorkflow<CType>["code"]
  ) => {
    const parStarts = workflow
      .map((code, line) => ({ code, line }))
      .filter(({ code, line }) => code.t === "par");

    const parNexts = new Set(
      parStarts
        .map(({ line: parLine }) => {
          const line = parLine + 1;
          const code = workflow.at(line);
          if (!code)
            throw new Error(
              "CParallelIndexer Error: no code after parallel opening"
            );

          // NOTE: only support event in parallel right now
          if (code.t !== "event") return null;

          return { code, line };
        })
        .filter((x): x is Exclude<typeof x, null> => x !== null)
        .map((x) => x.line)
    );

    return {
      isParallelStart: (line: number) => parNexts.has(line),
    };
  };
}

export namespace CCompensationIndexer {
  const construct = <CType extends CTypeProto>(
    workflow: WFWorkflow<CType>["code"]
  ) => {
    const compensateWithBlock: { start: number; end: number }[] = [];

    let compensationWithStartIndexes: number[] = [];
    workflow.forEach((line, index) => {
      if (line.t === "compensate-with") {
        compensationWithStartIndexes.push(index);
        return;
      } else if (line.t === "anti-compensate") {
        const start = compensationWithStartIndexes.pop();
        if (!start) return;
        compensateWithBlock.push({ start: start, end: index });
      }
    });

    return compensateWithBlock;
  };

  export const make = <CType extends CTypeProto>(
    workflow: WFWorkflow<CType>["code"]
  ) => {
    const list = construct(workflow);

    return {
      isInsideWithBlock: (x: number) =>
        list.findIndex((entry) => x > entry.start && x < entry.end) !== -1,
    };
  };
}
