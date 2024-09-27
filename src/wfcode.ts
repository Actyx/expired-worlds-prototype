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
import { LazyMap } from "./utils.js";

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
  | CParallelChildClose
  | CCompensate
  | CCompensateEnd
  | CCompensateWith
  | CMatch<CType>
  | CMatchCase
  | CChoice
  | CChoiceBarrier;

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
export namespace Actor {
  export const eq = <CType extends CTypeProto>(
    a: Actor<CType>,
    b: Actor<CType>
  ): boolean => a.t === b.t && a.get() === b.get();
}

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
export type CChoiceBarrier = { t: "choice-barrier"; antiIndexOffset: number };
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
export type CParallelChildClose = {
  t: "par-child-close";
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
    { t: "choice", antiIndexOffset: events.length * 2 + 1 },
    ...events.flatMap((e, index): CItem<CType>[] => [
      e,
      { t: "choice-barrier", antiIndexOffset: (events.length - index) * 2 - 1 },
    ]),
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
      pairOffsetIndex: workflow.length + 2,
      firstEventIndex: (() => {
        const firstEventIndex = workflow.findIndex((e) => e.t === "event");
        if (firstEventIndex === -1) throw new Error("ev not found");
        return 1 + firstEventIndex;
      })(),
    },
    ...workflow,
    { t: "par-child-close" },
    { t: "anti-par", pairOffsetIndex: workflow.length + 2 },
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
  // TODO: validate RETRY and TIMEOUT not jumping outside of compensate-with block

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
  export type Type<CType extends CTypeProto> = ReturnType<typeof make<CType>>;
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

    const list = extractor(workflow).extractPairs(
      (line) => line.t === "par",
      (line) => line.t === "anti-par"
    );

    return {
      list,
      parallelStarts: parStarts,
      isParallelStart: (line: number) => parNexts.has(line),
      getListMatching: (x: number) =>
        list.filter((entry) => x > entry.start && x < entry.end),
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
  export type Type<CType extends CTypeProto> = ReturnType<typeof make<CType>>;
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
  export type Type<CType extends CTypeProto> = ReturnType<typeof make<CType>>;
  export const make = <CType extends CTypeProto>(
    workflow: WFWorkflow<CType>["code"]
  ) => {
    const timeoutBlock = extractor(workflow).extractPairs(
      (line) => line.t === "timeout",
      (line) => line.t === "timeout-gap"
    );

    const afterTimeoutBlock = extractor(workflow).extractPairs(
      (line) => line.t === "timeout-gap",
      (line) => line.t === "anti-timeout"
    );

    return {
      getListMatching: (x: number) =>
        timeoutBlock.filter((entry) => x >= entry.start && x < entry.end),
      getAfterGapMatching: (x: number) =>
        afterTimeoutBlock.filter((entry) => x > entry.start && x < entry.end),
    };
  };
}

export namespace CRetryIndexer {
  export type Type<CType extends CTypeProto> = ReturnType<typeof make<CType>>;
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

export namespace ActorSet {
  export type Type<CType extends CTypeProto> = {
    add: (actor: Actor<CType>) => void;
    map: Actor<CType>[]["map"];
    forEach: Actor<CType>[]["forEach"];
    appendPatch: (patch: Type<CType>) => void;
    clone: () => Type<CType>;
    toArray: () => Actor<CType>[];
  };

  export const make = <CType extends CTypeProto>(
    input: Actor<CType>[] = []
  ): Type<CType> => {
    const inner = [] as Actor<CType>[];

    const self: Type<CType> = {
      add: (actor: Actor<CType>) => {
        const foundIndex = inner.findIndex((x) => Actor.eq(actor, x));
        if (foundIndex === -1) {
          inner.push(actor);
        }
      },
      map: (...params) => inner.map(...params),
      forEach: (...params) => inner.forEach(...params),
      appendPatch: (patch) => patch.forEach((x) => self.add(x)),
      clone: () => make(inner),
      toArray: () => [...inner],
    };

    input.forEach((x) => self.add(x));

    return self;
  };
}

export namespace CodeGraph {
  export type BranchOf<CType extends CTypeProto> = {
    chain: number[];
    terminating: boolean;
    involved: ActorSet.Type<CType>;
  };

  export const generateInvolvements = <CType extends CTypeProto>(
    workflow: WFWorkflow<CType>["code"],
    lookaheadsMap: Lookahead[][]
  ) => {
    const actorMap: ActorSet.Type<CType>[] = [];

    const terminuses = new Set(
      lookaheadsMap
        .map((lookaheads, index) => [lookaheads, index] as const)
        .filter(([lookaheads, _]) => lookaheads.length === 0)
        .map(([_, index]) => index)
    );

    const lookaheadsEdges = lookaheadsMap.flatMap((lookaheads, self) =>
      lookaheads.map((lookahead) => ({
        prev: self,
        next: lookahead.index,
      }))
    );

    const startBackwardLinearExtraction = (
      index: number,
      journey: Set<number>,
      nextActorSet: ActorSet.Type<CType> | null
    ) => {
      while (true) {
        if (journey.has(index)) return;
        journey.add(index);

        nextActorSet = extractActorAtIndex(index, nextActorSet);

        const prevNodes = lookaheadsEdges
          .filter((lookaheads) => lookaheads.next === index)
          .map((x) => x.prev);

        if (prevNodes.length !== 1) {
          return prevNodes.forEach((prev) => {
            startBackwardLinearExtraction(prev, new Set(journey), nextActorSet);
          });
        } else {
          index = prevNodes[0];
        }
      }
    };

    const extractActorAtIndex = (
      index: number,
      nextActorSet: ActorSet.Type<CType> | null
    ) => {
      const actorSet = actorMap[index] || ActorSet.make<CType>();
      actorMap[index] = actorSet;

      if (nextActorSet) actorSet.appendPatch(nextActorSet);

      const code = workflow.at(index);
      if (code?.t === "event") {
        actorSet.add(code.actor);
      }

      if (code?.t === "par") {
        const firstEventIndex = index + 1;
        const eventsCount = code.pairOffsetIndex - 1;
        workflow
          .slice(firstEventIndex, firstEventIndex + eventsCount)
          .forEach((item) => {
            if (item.t === "event") {
              actorSet.add(item.actor);
            }
          });
      }

      if (code?.t === "match") {
        Object.entries(code.args).forEach(([key, value]) => {
          actorSet.add(Unique(value));
        });
      }

      return actorSet;
    };

    // backward traversal from the terminus
    terminuses.forEach((index) => {
      startBackwardLinearExtraction(index, new Set(), null);
    });

    const baseActorSet = Array.from(actorMap.values()).reduce((acc, x) => {
      acc.appendPatch(x);
      return acc;
    }, ActorSet.make<CType>());

    return { baseActorSet, actorMap, terminuses };
  };

  export type Type<CType extends CTypeProto> = {
    code: WFWorkflow<CType>["code"];
    baseNexts: Nexts<CType>;
    nextMap: Nexts<CType>[];
    baseLookahead: Lookaheads;
    lookaheadsMap: Lookaheads[];
    baseActorSet: ActorSet.Type<CType>;
    actorSetMap: ActorSet.Type<CType>[];
    legSlideMap: number[];
    branchesFrom: (index: number) => BranchOf<CType>[];
    assignAheadsMap: AssignAheadsMap;
    terminuses: Set<number>;
  };
  export type NextCEvent<CType extends CTypeProto> = {
    index: number;
    code: CEvent<CType>;
    tags: LookaheadTagMap;
  };
  export type NextCCanonize = {
    index: number;
    code: CCanonize;
  };
  export type Next<CType extends CTypeProto> =
    | NextCEvent<CType>
    | NextCCanonize;

  export type Nexts<CType extends CTypeProto> = Next<CType>[];
  export type AssignAheads = { index: number }[];
  export type AssignAheadsMap = AssignAheads[];
  type LookbackCEvent<CType extends CTypeProto> = {
    index: number;
    code: CEvent<CType>;
  };

  type LookaheadTagMap = {
    compensationMainEntry?: { compensationIndex: number };
    compensationEntry?: { compensationIndex: number };
    compensationExit?: {};
    matchCase?: {};
    parallelInstantiation?: {};
    parallelContinuation?: {};
    timeoutEntry?: { timeoutIndex: number };
    timeoutJump?: { timeoutIndex: number };
    retryPatch?: { retryIndex: number };
  };
  type Lookahead = { index: number; tags: LookaheadTagMap };
  type Lookaheads = Lookahead[];

  export const generateLookaheadsMap = <CType extends CTypeProto>(
    workflow: WFWorkflow<CType>["code"],
    indices: {
      ctimeoutIndexer: ReturnType<typeof CTimeoutIndexer.make<CType>>;
      ccompensateIndexer: ReturnType<typeof CCompensationIndexer.make<CType>>;
      cretryIndexer: ReturnType<typeof CRetryIndexer.make<CType>>;
      cparallelIndexer: ReturnType<typeof CParallelIndexer.make<CType>>;
    }
  ) => {
    const {
      ctimeoutIndexer,
      ccompensateIndexer,
      cparallelIndexer,
      cretryIndexer,
    } = indices;

    const impl = (legIndex: number, c: CItem<CType>): Lookahead[] => {
      if (c.t === "anti-choice") {
        return [{ index: legIndex + 1, tags: {} }];
      }
      if (c.t === "anti-compensate") {
        return [
          {
            index: legIndex + c.baseIndexOffset + 1,
            tags: { compensationExit: {} },
          },
        ];
      }
      if (c.t === "anti-match-case") {
        return [
          {
            index: legIndex + c.afterIndexOffset,
            tags: {},
          },
        ];
      }
      if (c.t === "anti-retry") {
        return [{ index: legIndex + 1, tags: {} }];
      }
      if (c.t === "anti-timeout") {
        return [{ index: legIndex + c.pairOffsetIndex + 1, tags: {} }];
      }
      if (c.t === "canonize") {
        return [{ index: legIndex + 1, tags: {} }];
      }
      if (c.t === "choice") {
        const anti = c.antiIndexOffset;
        const events: number[] = [];
        let p = legIndex + 1;
        while (p < anti) {
          events.push(p);
          p += 2;
        }
        return events.map((index) => ({ index, tags: {} }));
      }
      if (c.t === "choice-barrier") {
        return [{ index: legIndex + c.antiIndexOffset + 1, tags: {} }];
      }
      if (c.t === "compensate") {
        return [
          {
            index: legIndex + 1,
            tags: { compensationMainEntry: { compensationIndex: legIndex } },
          },
        ];
      }
      if (c.t === "compensate-end") {
        return [{ index: legIndex + c.antiIndexOffset + 1, tags: {} }];
      }
      if (c.t === "compensate-with") {
        return [{ index: legIndex + 1, tags: {} }];
      }
      if (c.t === "match") {
        return c.casesIndexOffsets
          .map((offset) => legIndex + offset)
          .map((index) => {
            return {
              index: index,
              tags: { matchCase: {} } satisfies LookaheadTagMap,
            };
          });
      }
      if (c.t === "match-case") {
        return [{ index: legIndex + 1, tags: {} }];
      }
      if (c.t === "retry") {
        return [{ index: legIndex + 1, tags: {} }];
      }
      if (c.t === "timeout") {
        return [
          {
            index: legIndex + 1,
            tags: { timeoutEntry: { timeoutIndex: legIndex } },
          },
        ];
      }
      if (c.t === "timeout-gap") {
        return [{ index: legIndex + c.antiOffsetIndex + 1, tags: {} }];
      }
      if (c.t === "par") {
        return [
          { index: legIndex + 1, tags: { parallelInstantiation: {} } },
          { index: legIndex + c.pairOffsetIndex + 1, tags: {} },
        ];
      }
      if (c.t === "par-child-close") {
        return [];
      }
      if (c.t === "anti-par") {
        return [{ index: legIndex + 1, tags: {} }];
      }

      if (c.t !== "event") throw new Error("early returns not complete");
      // if (c.t === "event")

      if (c.control === Code.Control.return) {
        return [];
      }

      const parallel = cparallelIndexer.getListMatching(legIndex);
      if (parallel.length > 1) {
        throw new Error(
          `Nested parallel is not allowed at: ${parallel
            .map((x) => `${x.start}-${x.end}`)
            .join(", ")}`
        );
      }

      // if inside parallel, only move forwards
      if (parallel.length === 1) {
        return [{ index: legIndex + 1, tags: { parallelContinuation: {} } }];
      }

      const boundingCompsWith = ccompensateIndexer
        .getWithListMatching(legIndex)
        .sort((a, b) => b.start - a.start)
        .at(0);

      const direct: Lookahead[] = (() => {
        if (c.control === Code.Control.fail) {
          const first = cretryIndexer
            .getListMatching(legIndex)
            .filter((x) => {
              // filter timeout when inCompsWith
              if (boundingCompsWith) {
                return (
                  x.start > boundingCompsWith.start &&
                  x.end < boundingCompsWith.end
                );
              }
              return true;
            })
            .sort((a, b) => b.start - a.start)
            .at(0);
          if (!first) return [];
          return [
            {
              index: first.start,
              tags: { retryPatch: { retryIndex: first.start } },
            },
          ];
        }
        return [{ index: legIndex + 1, tags: {} }];
      })();

      return direct;
    };

    const determineLookaheads = (
      legIndex: number,
      code: CItem<CType>
    ): Lookahead[] => {
      const boundingCompsWith = ccompensateIndexer
        .getWithListMatching(legIndex)
        .sort((a, b) => b.start - a.start)
        .at(0);

      const direct = impl(legIndex, code).filter(
        (x) => x.index < workflow.length
      );

      // if inside CompsWith, an actor isn't elligible for compensation jumps until it is finished
      const compJumps: Lookahead[] = boundingCompsWith
        ? []
        : ccompensateIndexer.mainList
            // why x.end - 1? x.end indicates the compensate-with line
            // and the compensate-with is that once the leg is on the last point before compensate-with
            // the compensateable is actually done executing.
            .filter((x) => legIndex >= x.start && legIndex < x.end - 1)
            .sort((a, b) => b.start - a.start)
            .map((item) => {
              const index = item.start;
              const ccompensate = workflow.at(index);
              if (ccompensate?.t !== "compensate") {
                throw new Error(
                  `compensate query fatal error: ccompensate not found at index ${index}`
                );
              }

              const firstCompensationIndex =
                index + ccompensate.withIndexOffset + 1;
              const firstCompensation = workflow.at(firstCompensationIndex);

              if (firstCompensation?.t !== "event") {
                throw new Error(
                  `compensate query fatal error: compensation.with first code is not of type event`
                );
              }

              return firstCompensationIndex;
            })
            .map((index) => ({
              index,
              tags: {
                compensationEntry: { compensationIndex: index },
              } satisfies LookaheadTagMap,
            }));

      const timeouts: Lookahead[] = ctimeoutIndexer
        .getListMatching(legIndex)
        .filter((x) => {
          // filter timeout when inCompsWith
          if (boundingCompsWith) {
            return (
              x.start > boundingCompsWith.start && x.end < boundingCompsWith.end
            );
          }
          return true;
        })
        .map((pair) => {
          const timeoutIndex = pair.start;
          const gapIndexOffset = (workflow.at(pair.start) as CTimeout)
            .gapOffsetIndex;
          const gapIndex = timeoutIndex + gapIndexOffset;
          const afterGapIndex = gapIndex + 1;
          if (legIndex > gapIndex) return null;
          return afterGapIndex;
        })
        .filter((x): x is NonNullable<typeof x> => x !== null)
        .map((index) => ({
          index,
          tags: {
            timeoutJump: { timeoutIndex: index },
          } satisfies LookaheadTagMap,
        }));

      return direct.concat(timeouts).concat(compJumps);
    };

    return workflow.map((code, index) => determineLookaheads(index, code));
  };

  export const generateAssignAheadMap = <CType extends CTypeProto>(
    workflow: WFWorkflow<CType>["code"],
    lookaheadsMap: Lookaheads[]
  ): AssignAheadsMap => {
    const findEventLookback = (examinedIndex: number) => {
      let findLookbackIndex = examinedIndex - 1;

      while (examinedIndex > 0 && findLookbackIndex > 0) {
        const matchingLookahead = lookaheadsMap
          .at(findLookbackIndex)
          ?.find((x) => x.index === examinedIndex);

        if (matchingLookahead) {
          const codeAtFindLookback = workflow.at(findLookbackIndex);
          if (codeAtFindLookback?.t === "event") {
            return { index: findLookbackIndex, code: codeAtFindLookback };
          } else {
            examinedIndex = findLookbackIndex;
            findLookbackIndex = examinedIndex - 1;
          }
        } else {
          findLookbackIndex--;
        }
      }

      return null;
    };

    const determineLookbackCEvent = (
      legIndex: number
    ): { value: LookbackCEvent<CType> | null } | { error: string } => {
      const code = workflow.at(legIndex);
      if (code?.t !== "canonize" && code?.t !== "par") return { value: null };

      const eventLookback = findEventLookback(legIndex);
      if (!eventLookback) {
        return {
          error: `lookback for ${legIndex} is not found ${JSON.stringify(
            code
          )}`,
        };
      }

      return { value: eventLookback };
    };

    const lookbackMapRes = workflow.map((_, index) =>
      determineLookbackCEvent(index)
    );

    const errors = lookbackMapRes
      .map((x) => {
        if ("error" in x) {
          return x.error;
        }
        return null;
      })
      .filter((x): x is Exclude<typeof x, null> => x !== null);

    if (errors.length > 0) {
      throw new Error(
        [`Lookback not found errors: `, ...errors.map((x) => `-${x}`)].join(
          "\n"
        )
      );
    }

    const assignAheadsMap: AssignAheadsMap = new Array(workflow.length).fill(
      []
    );
    lookbackMapRes.forEach((lookback, assignIndex) => {
      if ("value" in lookback) {
        const lookbackValue = lookback.value;
        if (lookbackValue) {
          const assignAheads = assignAheadsMap[assignIndex];
          assignAheads.push({ index: lookbackValue.index });
        }
      }
    });

    return assignAheadsMap;
  };

  /**
   * LegSlide is a index to index mapping where the leg of a WFMachine can
   * "slide" into the next index. For example a leg on a CEvent can "slide"
   * freely into the next CTimeout or CRetry, but it cannot "slide" to the next
   * CEvent as a CEvent costs something to be "stepped on"
   */
  export const generateLegslideMap = <CType extends CTypeProto>(
    workflow: WFWorkflow<CType>["code"],
    lookaheadsMap: Lookaheads[]
  ) => {
    const legSlide = [] as number[];

    const canSlideTo = (lookahead: Lookahead) => {
      const { tags } = lookahead;
      if (
        tags.compensationEntry ||
        tags.compensationExit ||
        tags.matchCase ||
        tags.parallelContinuation ||
        tags.parallelInstantiation
      )
        return false;
      const code = workflow.at(lookahead.index);
      if (!code) return false;
      if (code.t === "event") return false;
      return true;
    };

    const extractLegslideFrom = (
      legIndex: number
    ): { from: number[]; lastSlidePoint: number } => {
      const from = [] as number[];

      let lastSlidePoint = legIndex;
      while (true) {
        from.push(lastSlidePoint);

        const lookaheads = lookaheadsMap.at(lastSlidePoint);
        if (!lookaheads) return { from, lastSlidePoint };

        const validRightAfterLookahead = lookaheads
          .filter(
            (lahead) =>
              lahead.index === lastSlidePoint + 1 && canSlideTo(lahead)
          )
          .at(0);

        if (validRightAfterLookahead) {
          lastSlidePoint = validRightAfterLookahead.index;
          continue;
        }

        return { from, lastSlidePoint };
      }
    };

    workflow.forEach((_, index) => {
      const existingLegslide = legSlide.at(index);
      if (existingLegslide === undefined) {
        const extracted = extractLegslideFrom(index);
        extracted.from.forEach((index) => {
          legSlide[index] = extracted.lastSlidePoint;
        });
      }
    });

    return legSlide;
  };

  export const generateNextsMap = <CType extends CTypeProto>(
    workflow: WFWorkflow<CType>["code"],
    lookaheadsMap: Lookaheads[]
  ): { nextMap: Nexts<CType>[]; baseNexts: Nexts<CType> } => {
    type NextDeterminator =
      | {
          t: "lookingahead";
          lookahead: Lookahead;
          tags: LookaheadTagMap;
        }
      | {
          t: "final";
          item: Next<CType>;
        };

    const determineNexts = (legIndex: number) => {
      const traverseNextDeterminator = (
        det: NextDeterminator
      ): NextDeterminator[] => {
        if (det.t === "final") {
          return [det];
        }
        if (det.t === "lookingahead") {
          const code = workflow.at(det.lookahead.index);
          if (!code) return [];
          const nextIndex = det.lookahead.index;
          // the stopping node is where it takes an input to step into
          if (code.t === "event") {
            return [
              {
                t: "final",
                item: { index: nextIndex, code: code, tags: det.tags },
              },
            ];
          }

          const nextLookaheads = lookaheadsMap.at(nextIndex) || [];
          return nextLookaheads.map(
            (lookahead) =>
              ({
                t: "lookingahead",
                lookahead,
                tags: { ...det.tags, ...lookahead.tags },
              } satisfies NextDeterminator)
          );
        }
        return [];
      };

      let branches: NextDeterminator[] =
        lookaheadsMap.at(legIndex)?.map((lookahead) => ({
          t: "lookingahead",
          lookahead,
          tags: { ...lookahead.tags },
        })) || [];

      while (branches.find((x) => x.t === "lookingahead") !== undefined) {
        branches = branches.flatMap(traverseNextDeterminator);
      }

      return branches.flatMap((det) => {
        if (det.t === "final") {
          return [det.item];
        }
        return [];
      });
    };

    const baseCEvent = workflow.at(0);
    if (baseCEvent?.t !== "event") {
      throw new Error("first CEvent cannot be non-event");
    }
    const baseNexts: Next<CType>[] = [
      {
        index: 0,
        code: baseCEvent,
        tags: {},
      },
    ];
    const nextMap = workflow.map((_, index) => determineNexts(index));

    return { nextMap, baseNexts: baseNexts };
  };

  export type CompilerCache<CType extends CTypeProto> = {
    codegraph: LazyMap.Type<WFWorkflow<CType>["code"], Type<CType>>;
    indexer: {
      timeout: LazyMap.Type<
        WFWorkflow<CType>["code"],
        CTimeoutIndexer.Type<CType>
      >;
      compensation: LazyMap.Type<
        WFWorkflow<CType>["code"],
        CCompensationIndexer.Type<CType>
      >;
      retry: LazyMap.Type<WFWorkflow<CType>["code"], CRetryIndexer.Type<CType>>;
      parallel: LazyMap.Type<
        WFWorkflow<CType>["code"],
        CParallelIndexer.Type<CType>
      >;
    };
  };

  export const make = <CType extends CTypeProto>(
    workflow: WFWorkflow<CType>["code"],
    compilerCache: CompilerCache<CType>
  ): CodeGraph.Type<CType> => {
    const ccompensateIndexer = compilerCache.indexer.compensation.get(workflow);
    const ctimeoutIndexer = compilerCache.indexer.timeout.get(workflow);
    const cretryIndexer = compilerCache.indexer.retry.get(workflow);
    const cparallelIndexer = compilerCache.indexer.parallel.get(workflow);

    // Lookahead and nexts determinations

    const baseLookahead: Lookahead = { index: 0, tags: {} };
    const lookaheadsMap = generateLookaheadsMap(workflow, {
      ccompensateIndexer,
      ctimeoutIndexer,
      cretryIndexer,
      cparallelIndexer,
    });

    const { nextMap, baseNexts } = generateNextsMap(workflow, lookaheadsMap);

    const assignAheadsMap = generateAssignAheadMap(workflow, lookaheadsMap);

    const { actorMap, baseActorSet, terminuses } = generateInvolvements(
      workflow,
      lookaheadsMap
    );

    const legSlideMap = generateLegslideMap(workflow, lookaheadsMap);

    const continueNonLoopBranches = (chain: number[]): number[][] => {
      while (true) {
        const last = chain.at(chain.length - 1);
        // weird as there's no last item in the input array
        // return empty
        if (last === undefined) return [chain];

        const lookaheads = (() => {
          if (last === -1) return [baseLookahead];
          return lookaheadsMap.at(last);
        })();

        if (!lookaheads) return [chain];

        if (lookaheads.length === 1) {
          const next = lookaheads[0].index;
          if (chain.includes(next)) return [chain]; // is a loop, ignore this branch
          chain.push(next);
        } else {
          return lookaheads.reduce((acc: number[][], nextLookahead) => {
            const next = nextLookahead.index;
            if (!chain.includes(next)) {
              acc.push([...chain]);
            } else {
              acc.push(...continueNonLoopBranches([...chain, next]));
            }
            return acc;
          }, []);
        }
      }
    };
    // lazy cache for infomap
    const lazyBranchInfoMap = new Map<number, BranchOf<CType>[]>();

    const branchesFrom = (index: number) => {
      const branchInfoArr =
        lazyBranchInfoMap.get(index) ||
        continueNonLoopBranches([index])
          .filter((chain) => chain.length > 0)
          .map((chain): BranchOf<CType> => {
            const involved = ActorSet.make<CType>();

            chain.forEach((node) => {
              const involvedInThisNode = actorMap.at(node);
              if (!involvedInThisNode) return;
              involved.appendPatch(involvedInThisNode);
            });

            return {
              chain,
              involved,
              terminating: (() => {
                const last = chain.at(chain.length - 1);
                if (last === undefined) return false;
                return terminuses.has(last);
              })(),
            };
          });

      if (!lazyBranchInfoMap.has(index)) {
        lazyBranchInfoMap.set(index, branchInfoArr);
      }

      return branchInfoArr;
    };

    const self: CodeGraph.Type<CType> = {
      code: workflow,
      baseNexts,
      nextMap,
      baseLookahead: [baseLookahead],
      lookaheadsMap,
      baseActorSet,
      actorSetMap: actorMap,
      legSlideMap,
      branchesFrom,
      assignAheadsMap: assignAheadsMap,
      terminuses,
    };

    return self;
  };

  export const toString = <CType extends CTypeProto>(cg: Type<CType>): string =>
    cg.code
      .map((code, index) => {
        const nexts = cg.nextMap.at(index) || [];
        const involved = cg.actorSetMap.at(index);
        return [
          `${index}:${JSON.stringify(code)}`,
          `  nexts: ${nexts.map((x) => x.index).join(", ")}`,
          `  involved: ${
            (involved &&
              involved
                .toArray()
                .map((x) => `${x.t}:${x.get()}`)
                .join(", ")) ||
            "none"
          }`,
        ].join("\n");
      })
      .join("\n");
}
