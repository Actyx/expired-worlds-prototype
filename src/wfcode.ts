/**
 * @module
 *
 * Definitions and constructors for CItem "bytecode" that the WFMachine runs.
 *
 * Some design principles that has been useful for designing CItem subtypes:
 * - "time flows downwards": the CItem below will be inspected later than the
 *   one above. This helps minimize the need for the WFMachine to jump back or
 *   reinspect previous code indices.
 * - "indexable": A line should include no more than one CItem. A CItem should
 *   be indexable directly, thus can have a stack counterpart. (in the rust
 *   version, the stack counterpart can rest together with the CItem as an
 *   option).
 * - "granular and unambiguous": When a subtype has too big of a responsibility,
 *   opt for creating more subtype. WFMachine can always autoEvaluate through
 *   two codes. Also, this principle will help because stateCalcIndex is always
 *   lower than evalIndex so that stateCalcIndex will not calculate the code
 *   pointed by evalIndex. (might change in the future)
 *
 * As an example, a timeout consequence used to be encoded as:
 *
 * ```
 * CTimeout { consequence: CEvent }
 *  ...CItem[]
 * CAntiTimeout
 * ```
 *
 * This causes a problem because the stack counterpart needs an SAntiTimeout to
 * store the event described in the consequence. In turn, a new function needs
 * to be defined to handle SAntiTimeout in a similar manner to SEvent.
 *
 * Then CTimeout was changed into this.
 *
 * ```
 * CTimeout
 * ...CItem[]
 * CTimeoutGap
 * CEvent           // the consequence
 * CAntiTimeout
 * ```
 *
 * This way, if an event matching the consequence is detected, WFMachine's
 * evalIndex only needs to populate SEvent at the same index as the consequence
 * CEvent using the same function to populate any other CEvent, and then jump
 * right below the CEvent.
 */

import { CTypeProto, NestedCodeIndexAddress } from "./consts.js";
import { Enum, WrapType } from "./utils.js";

/**
 * CItem is each "line" of the byte code
 */
export type CItem<CType extends CTypeProto> =
  | CCanonize
  | CAnti
  | CEvent<CType>
  | CRetry
  | CTimeout
  | CTimeoutGap
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

export const Unique = WrapType.blueprint("Unique").refine<string>().build();
export type Unique = WrapType.TypeOf<typeof Unique>;
export const Role = WrapType.blueprint("Role");
export type Role<Ctype extends CTypeProto> = WrapType.TypeOf<
  WrapType.Utils.Refine<typeof Role, Ctype["role"]>
>;

/**
 * An actor is either a Unique or a Role.
 */
export type Actor<CType extends CTypeProto> = Unique | Role<CType>;

export type CCanonize = {
  t: "canonize";
  actor: Unique;
};

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

/**
 * Match case is matched with the return value of the CMatch's sub-WFMachine.
 */
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
export type CTimeout = {
  t: "timeout";
  duration: number;
  antiOffsetIndex: number;
  gapOffsetIndex: number;
};

export type CTimeoutGap = { t: "timeout-gap"; antiOffsetIndex: number };

export type CAntiRetry = { t: "anti-retry"; pairOffsetIndex: number };
export type CAntiTimeout = { t: "anti-timeout"; pairOffsetIndex: number };
export type CAntiParallel = { t: "anti-par"; pairOffsetIndex: number };

/**
 * A collection of helper to write CItem.
 */
export namespace Code {
  /**
   * Marker for Control which concists of "fail" and "return".
   */
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
    canonize: typeof canonize;

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
    canonize,

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

  const canonize = (actor: Unique): CCanonize => ({
    t: "canonize",
    actor,
  });

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
        antiIndexOffset: 1 + compensation.length + 1,
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
  ): CItem<CType>[] => {
    if (consequence.control === undefined) {
      throw new Error(`timeout consequence must be fail or return`);
    }
    const gapOffsetIndex = workflow.length + 1;
    const antiOffsetIndex = gapOffsetIndex + 2;
    return [
      {
        t: "timeout",
        duration,
        gapOffsetIndex,
        antiOffsetIndex,
      },
      ...workflow,
      { t: "timeout-gap", antiOffsetIndex: 2 },
      consequence,
      { t: "anti-timeout", pairOffsetIndex: -antiOffsetIndex },
    ];
  };
}

/**
 * One of the input of WFMachine; it consists of UniqueParams and the code
 * itself. UniqueParams are used to bind identity variables from outer WFMachine
 * to inner WFMachine.
 */
export type WFWorkflow<CType extends CTypeProto> = {
  uniqueParams: string[];
  code: Readonly<[CEvent<CType>, ...CItem<CType>[]]>;
};

/**
 * Validations run at the beginning of WFMachine. This theoretically could be
 * run at the point when the WFWorkflow is created.
 */
export const validate = <CType extends CTypeProto>(
  workflow: WFWorkflow<CType>
) => {
  const errors = ([] as string[])
    .concat(validateBindings(workflow))
    .concat(validateCompensateNotFirstEvent(workflow))
    .concat(validateFirstItemIsEvent(workflow))
    .concat(validateCompensationFirstEvent(workflow))
    .concat(validateCompensationWithNotContainingRoleBasedActor(workflow))
    .concat(validateCanonize(workflow));

  if (errors.length > 0) {
    throw new Error(errors.map((x) => `- ${x.trim()}`).join("\n"));
  }
};

/**
 * A canonize's actor must be a unique which has been bound before the last
 * point (or any point maybe) that allows branching (timeout, event.fail /
 * event.return, parallel)
 *
 * Canonize cannot be inside a loop too since each canonization must only happen
 * once per workflow.
 *
 * Valid actor bindings and branching events are those outside of WITH-BLOCK
 */
export const validateCanonize = <CType extends CTypeProto>(
  workflow: WFWorkflow<CType>
) => {
  const errors = [] as string[];
  const par = CParallelIndexer.make(workflow.code);
  const timeouts = CTimeoutIndexer.make(workflow.code);
  const compensations = CCompensationIndexer.make(workflow.code);
  const indexedCodes = workflow.code.map((x, i) => [i, x] as const);
  const canonizeCodes = indexedCodes.filter(
    (pair): pair is [number, CCanonize] => pair[1]?.t === "canonize"
  );

  canonizeCodes.forEach((canonizePair) => {
    const actorDesignation = canonizePair[1].actor.get();

    const actorBinding = indexedCodes
      .filter(([index]) => !compensations.isInsideWithBlock(index))
      .reverse()
      .find(
        ([index, code]) =>
          index < canonizePair[0] &&
          code.t === "event" &&
          code.bindings?.find((x) => x.var === actorDesignation)
      );

    if (!actorBinding) {
      errors.push(
        `canonizer binding not found for unique actor ${actorDesignation}`
      );
      return;
    }

    const [bindingIndex] = actorBinding;

    par.parallelStarts
      .filter(({ line }) => line < bindingIndex)
      .forEach(({ line }) => {
        errors.push(
          `canonizer binding for ${actorDesignation} cannot be assigned after parallel at line ${line}`
        );
      });

    timeouts.getListMatching(bindingIndex).forEach((match) => {
      errors.push(
        `canonizer binding for ${actorDesignation} cannot be assigned inside a timeout starting at ${match.start}`
      );
    });

    indexedCodes
      .filter(([index]) => !compensations.isInsideWithBlock(index))
      .filter(([index]) => index < bindingIndex)
      .forEach(([_index, code]) => {
        if (
          code.t === "event" &&
          (code.control === Code.Control.fail ||
            code.control === Code.Control.return)
        ) {
          errors.push(
            `canonizer binding for ${actorDesignation} cannot be assigned after failing and returning event`
          );
        }
      });
  });

  return errors;
};

/**
 * Compensate needs eventId as anchor
 */
export const validateCompensateNotFirstEvent = <CType extends CTypeProto>(
  workflow: WFWorkflow<CType>
): string[] => {
  if (workflow.code.at(0)?.t === "compensate") {
    return [`first code cannot be compensate.`];
  }
  return [];
};

export const validateBindings = <CType extends CTypeProto>(
  workflow: WFWorkflow<CType>
): string[] => {
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

  return errors;
};

export const validateFirstItemIsEvent = <CType extends CTypeProto>(
  workflow: WFWorkflow<CType>
): string[] => {
  const code: ReadonlyArray<CItem<CType>> = workflow.code;
  if (code.at(0)?.t !== "event") return ["first item cannot be non-event"];
  return [];
};

export const validateCompensationFirstEvent = <CType extends CTypeProto>(
  workflow: WFWorkflow<CType>
) => {
  const compensateWithIndices = workflow.code
    .map((x, i) => [i, x] as const)
    .filter(([i, x]) => x.t === "compensate-with")
    .map(([i]) => i);
  const firstCompIndicies = compensateWithIndices.map((x) => x + 1);
  const errors = firstCompIndicies
    .map((index) => {
      const item = workflow.code.at(index);
      return [index, item] as const;
    })
    .filter(([index, item]) => item?.t !== "event")
    .map(
      ([index]) =>
        `first item at ${index} must be an event because it is right after a compensate-with block`
    );

  return errors;
};

export const validateCompensationWithNotContainingRoleBasedActor = <
  CType extends CTypeProto
>(
  workflow: WFWorkflow<CType>
) => {
  const errors = [] as string[];

  const inspectCode = (
    level: NestedCodeIndexAddress.Type,
    codelines: (readonly [number, CItem<CType>])[]
  ) => {
    codelines
      .filter((line): line is [number, CEvent<CType>] => line[1].t === "event")
      .forEach(([index, code]) => {
        if (code.actor.t === "Role") {
          errors.push(
            `Event inside a compensation's with block cannot have a role actor. Error at index ${[
              ...level,
              index,
            ].join(".")}, code: ${JSON.stringify(code)}`
          );
        }
      });

    codelines
      .filter((line): line is [number, CMatch<CType>] => line[1].t === "match")
      .forEach(([index, code]) =>
        inspectCode(
          [...level, index],
          code.subworkflow.code.map((x, i) => [i, x] as const)
        )
      );
  };

  CCompensationIndexer.make(workflow.code).withList.forEach((pair) =>
    inspectCode(
      [],
      workflow.code
        .map((x, i) => [i, x] as const)
        .slice(pair.start + 1, pair.end)
    )
  );

  return errors;
};

/**
 * Index WFWorkflow for Parallel codes for faster queries with better-defined
 * APIs. This is useful for queries done inside WFMachine.
 */
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
      parallelStarts: parStarts,
      isParallelStart: (line: number) => parNexts.has(line),
    };
  };
}

const extractor = <CType extends CTypeProto>(
  workflow: WFWorkflow<CType>["code"]
) => {
  const extractPairs = (
    heurBegin: (line: CItem<CType>) => boolean,
    heurEnd: (line: CItem<CType>) => boolean
  ) => {
    const pairEntries: { start: number; end: number }[] = [];
    let unendedStartIndices: number[] = [];

    workflow.forEach((line, index) => {
      if (heurBegin(line)) {
        unendedStartIndices.push(index);
        return;
      } else if (heurEnd(line)) {
        const start = unendedStartIndices.pop();
        if (!start) return;
        pairEntries.push({ start: start, end: index });
      }
    });

    return pairEntries;
  };

  return { extractPairs };
};

/**
 * Index WFWorkflow for compensation codes for faster queries with
 * better-defined APIs. This is useful for queries done inside WFMachine.
 */
export namespace CCompensationIndexer {
  export const make = <CType extends CTypeProto>(
    workflow: WFWorkflow<CType>["code"]
  ) => {
    const mainList = extractor(workflow).extractPairs(
      (line) => line.t === "compensate",
      (line) => line.t === "compensate-end"
    );

    const withList = extractor(workflow).extractPairs(
      (line) => line.t === "compensate-with",
      (line) => line.t === "anti-compensate"
    );

    const involvementMap = new Map(
      mainList
        .map((pair) => [pair.start, workflow.at(pair.start)] as const)
        .filter(
          (line): line is [number, CCompensate] => line[1]?.t === "compensate"
        )
        .map(([compIndex, comp]) => {
          const { antiIndexOffset, withIndexOffset } = comp;

          const withIndex = compIndex + withIndexOffset;
          const antiIndex = compIndex + antiIndexOffset;
          const involvedBindings = workflow
            .slice(withIndex + 1, antiIndex)
            .filter(
              (x): x is CEvent<CType> | CMatch<CType> =>
                x.t === "event" || x.t === "match"
            )
            .flatMap((x) => {
              if (x.t === "event" && x.actor.t === "Unique") {
                return x.actor.get();
              }
              if (x.t === "match") {
                return Object.values(x.args);
              }
              return [];
            });

          return [compIndex, involvedBindings] as const;
        })
    );

    return {
      mainList,
      withList,
      involvementMap: involvementMap as ReadonlyMap<number, string[]>,
      getWithListMatching: (x: number) =>
        withList.filter((entry) => x > entry.start && x < entry.end),
      getMainListMatching: (x: number) =>
        mainList.filter((entry) => x > entry.start && x < entry.end),
      isInsideWithBlock: (x: number) =>
        withList.findIndex((entry) => x > entry.start && x < entry.end) !== -1,
    };
  };
}

export namespace CTimeoutIndexer {
  export const make = <CType extends CTypeProto>(
    workflow: WFWorkflow<CType>["code"]
  ) => {
    const list = extractor(workflow).extractPairs(
      (line) => line.t === "timeout",
      (line) => line.t === "anti-timeout"
    );

    return {
      getListMatching: (x: number) =>
        list.filter((entry) => x > entry.start && x < entry.end),
    };
  };
}

export namespace CRetryIndexer {
  export const make = <CType extends CTypeProto>(
    workflow: WFWorkflow<CType>["code"]
  ) => {
    const list = extractor(workflow).extractPairs(
      (line) => line.t === "retry",
      (line) => line.t === "anti-retry"
    );

    return {
      list,
      getListMatching: (x: number) =>
        list.filter((entry) => x > entry.start && x < entry.end),
    };
  };
}
