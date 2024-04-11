import { describe, expect, it } from "@jest/globals";

// TODO:
// - Participations?

const sleep = (x: number) => new Promise((res) => setTimeout(res, x));

const Enum = <T extends ReadonlyArray<string>>(
  strs: T
): Readonly<{ [S in T[number] as S]: Readonly<S> }> =>
  strs.reduce((acc, x) => {
    acc[x] = x;
    return acc;
  }, {} as any);
type Enum<T extends object> = T[keyof T];

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
  "unloaded",
  "reqStorage",
  "offerStorage",
  "assistanceNeeded",
  "atWarehouse",
  "stashed",
  "assistanceNeeded",
  "logisticFailed",
  "done",
] as const);
type Ev = Enum<typeof Ev>;

// Code
type makeCType<CType extends CTypeProto> = CType;
type CTypeProto = {
  ev: string;
  role: string;
};
type CItem<CType extends CTypeProto> =
  | CAnti
  | CEvent<CType>
  | CRetry
  | CTimeout<CType>
  | CParallel
  | CCompensate
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

type CEventBinding = { var: string; index: string };
type CEvent<CType extends CTypeProto> = {
  t: "event";
  name: CType["ev"];
  role: CType["role"];
  bindings?: CEventBinding[];
  control?: Code.Control;
};
type CChoice = { t: "choice"; antiIndexOffset: number };
type CAntiChoice = { t: "anti-choice" };
type CMatch<CType extends CTypeProto> = {
  t: "match";
  subworkflow: readonly [CEvent<CType>, ...CItem<CType>[]];
  casesIndexOffsets: number[];
};
const Name: unique symbol = Symbol("Name");
const Otherwise: unique symbol = Symbol("Otherwise");
type CMatchCaseType = [typeof Name, string] | [typeof Otherwise];
type CMatchCase = {
  t: "match-case";
  case: [typeof Name, string] | [typeof Otherwise];
};
type CAntiMatchCase = { t: "anti-match-case"; afterIndexOffset: number };
type CCompensate = {
  t: "compensate";
  withIndexOffset: number;
  antiIndexOffset: number;
};
type CCompensateWith = {
  t: "compensate-with";
  baseIndexOffset: number;
  antiIndexOffset: number;
};
type CAntiCompensate = { t: "anti-compensate"; baseIndexOffset: number };
type CParallel = {
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
type CRetry = { t: "retry"; pairOffsetIndex: number };
type CTimeout<CType extends CTypeProto> = {
  t: "timeout";
  duration: number;
  consequence: CEvent<CType>;
  pairOffsetIndex: number;
};

type CAntiRetry = { t: "anti-retry"; pairOffsetIndex: number };
type CAntiTimeout = { t: "anti-timeout"; pairOffsetIndex: number };
type CAntiParallel = { t: "anti-par"; pairOffsetIndex: number };

namespace Code {
  export const Control = Enum(["fail", "return"] as const);
  export type Control = Enum<typeof Control>;

  export type CodeMaker<CType extends CTypeProto> = {
    binding: typeof binding;
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
    binding,
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
    index,
    var: name,
  });

  const event = <CType extends CTypeProto>(
    role: CType["role"],
    name: CType["ev"],
    x?: Pick<CEvent<CType>, "bindings" | "control">
  ): CEvent<CType> => ({ t: "event", role, name, ...(x || {}) });

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

  const compensate = <CType extends CTypeProto>(
    main: [CEvent<CType>, ...CItem<CType>[]],
    compensation: [CEvent<CType>, ...CItem<CType>[]]
  ): CItem<CType>[] => {
    const withOffset = main.length + 1;
    const antiOffset = main.length + 1 + compensation.length + 1;
    return [
      {
        t: "compensate",
        withIndexOffset: withOffset,
        antiIndexOffset: antiOffset,
      },
      ...main,
      {
        t: "compensate-with",
        baseIndexOffset: withOffset * -1,
        antiIndexOffset: compensation.length + 1,
      },
      ...compensation,
      {
        t: "anti-compensate",
        baseIndexOffset: antiOffset * -1,
      },
    ];
  };

  const matchCase = <CType extends CTypeProto>(
    t: CMatchCaseType,
    item: readonly CItem<CType>[]
  ): [CMatchCaseType, readonly CItem<CType>[]] => [t, item];

  const match = <CType extends CTypeProto>(
    workflow: readonly [CEvent<CType>, ...CItem<CType>[]],
    cases: [CMatchCaseType, readonly CItem<CType>[]][]
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
      { t: "match", casesIndexOffsets: offsets, subworkflow: workflow },
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
        return firstEventIndex + 1;
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

// Stack
type StackItem<CType extends CTypeProto> =
  | SMatch<CType>
  | SEvent<CType>
  | SRetry
  | STimeout<CType>
  | SAntiTimeout<CType>
  | SParallel<CType>
  | SAntiParallel;

type SMatch<CType extends CTypeProto> = Pick<CMatch<CType>, "t"> & {
  inner: WFMachine<CType>;
};
type SEvent<CType extends CTypeProto> = Pick<CEvent<CType>, "t"> & {
  payload: EEvent<CType>["payload"];
};
type SRetry = Pick<CRetry, "t">;
type SParallelExecution<CType extends CTypeProto> = { entry: EEvent<CType>[] };
type SParallel<CType extends CTypeProto> = Pick<CParallel, "t"> & {
  fulfilled: boolean;
  nextEvalIndex: number;
  instances: SParallelExecution<CType>[];
};
type STimeout<CType extends CTypeProto> = Pick<CTimeout<CType>, "t"> & {
  startedAt: Date;
};
type SAntiTimeout<CType extends CTypeProto> = Pick<CAntiTimeout, "t"> & {
  consequence: CTimeout<CType>["consequence"];
  data: EEvent<CType>;
};
type SAntiParallel = Pick<CAntiParallel, "t"> & {};

// Payload

type EEvent<CType extends CTypeProto> = {
  t: "event";
  name: CType["ev"];
  payload: Record<string, unknown>;
};
namespace Emit {
  export const event = <CType extends CTypeProto>(
    name: CType["ev"],
    payload: Record<string, unknown>
  ): EEvent<CType> => ({
    t: "event",
    name,
    payload,
  });
}

const One: unique symbol = Symbol("One");
const Parallel: unique symbol = Symbol("Parallel");
type One<T = unknown> = [typeof One, T];
type Parallel<T = unknown> = [typeof Parallel, T[]];

type State<T> = One<T> | Parallel<T>;

type WFMachine<CType extends CTypeProto> = {
  tick: (state: EEvent<CType> | null) => boolean;
  state: () => {
    state: State<CType["ev"]> | null;
    context: Record<string, unknown>;
  };
  returned: () => boolean;
  availableTimeout: () => {
    consequence: {
      name: CType["ev"];
      control: Code.Control | undefined;
    };
    dueFor: number;
  }[];
  availableCompensations: () => {
    name: CType["ev"];
  }[];
  availableCommands: () => {
    role: CType["role"];
    name: CType["ev"];
    control?: Code.Control;
    reason: null | "compensation" | "timeout";
  }[];
};

const WFMachine = <CType extends CTypeProto>(
  workflow: Readonly<[CEvent<CType>, ...CItem<CType>[]]>
): WFMachine<CType> => {
  const data = {
    executionIndex: 0,
    stack: [] as (StackItem<CType> | null)[],

    activeTimeout: new Set() as Set<number>,
    activeCompensation: new Set() as Set<number>,

    resultCalcIndex: 0,
    context: {} as Record<string, unknown>,
    returnValue: null as State<CType["ev"]> | null,
    returned: false,
  };

  const resetIndex = (evalContext: EvalContext, targetIndex: number) => {
    // set execution back at the index
    data.stack.length = targetIndex;
    data.executionIndex = targetIndex;
    evalContext.index = targetIndex;

    // remove timeouts after the last item index in stack
    data.activeTimeout = new Set(
      Array.from(data.activeTimeout).filter(
        (index) => index >= data.stack.length
      )
    );

    // force recalculate context
    data.resultCalcIndex = 0;
    data.context = {};
  };

  // Finders helpers

  const nullifyMatchingTimeout = (
    antiTimeout: CAntiTimeout,
    indexInput: number
  ) => {
    const timeoutIndex = antiTimeout.pairOffsetIndex + indexInput;
    const maybeTimeout = data.stack.at(timeoutIndex);

    if (data.activeTimeout.has(timeoutIndex) && maybeTimeout?.t === "timeout") {
      data.activeTimeout.delete(timeoutIndex);
      data.activeTimeout.delete(timeoutIndex);
      data.stack[timeoutIndex] = null;
    } else {
      throw new Error("timeout not found on stack");
    }
  };

  const findMatchingAntiTimeout = (
    timeout: CTimeout<CType>,
    currentIndex: number
  ) => {
    const pairOffset = timeout.pairOffsetIndex;
    const pairIndex = currentIndex + pairOffset;
    const code = workflow.at(pairIndex);
    if (code?.t === "anti-timeout") {
      return pairIndex;
    }
    return null;
  };

  const findMatchingRetryIndex = (retry: CAntiRetry, indexInput: number) => {
    const pairIndex = retry.pairOffsetIndex + indexInput;
    const code = workflow.at(pairIndex);
    if (code?.t === "retry") {
      return pairIndex;
    }
    return null;
  };

  const findRetryOnStack = (indexInput: number) => {
    let index = indexInput;
    while (index > 0) {
      index -= 1;
      const stackItem = data.stack.at(index);
      if (stackItem?.t === "retry") {
        return index;
      }
    }

    return null;
  };

  const getSMatchAtIndex = (index: number) => {
    const atStack = data.stack.at(index);
    if (!atStack) {
      return null;
    }

    if (atStack.t !== "match") {
      throw new Error("match stack position filled with non-match");
    }

    return atStack;
  };

  // Helpers

  /**
   * Find all active timeouts
   */
  const availableTimeout = () =>
    Array.from(data.activeTimeout)
      .map((index) => {
        const ctimeout = workflow.at(index);
        const stimeout = data.stack.at(index);
        if (ctimeout?.t !== "timeout") {
          throw new Error(
            `attempt timeout fatal error: ctimeout not found at index ${index}`
          );
        }
        if (stimeout?.t !== "timeout") {
          throw new Error(
            `timeout query fatal error: stimeout not found at index ${index}`
          );
        }

        const antiIndex = index + ctimeout.pairOffsetIndex;
        const dueDate = stimeout.startedAt.getTime() + ctimeout.duration;
        const lateness = Date.now() - dueDate;

        return { stimeout, ctimeout, lateness, antiIndex };
      })
      .filter(({ lateness }) => lateness > 0)
      .sort((a, b) => {
        // in case of nested timeouts: sorted by last / outermost timeout
        return b.antiIndex - a.antiIndex;
      });

  /**
   * Find all active compensations
   */
  const availableCompensations = () =>
    Array.from(data.activeCompensation)
      .map((index) => {
        const ctimeout = workflow.at(index);
        if (ctimeout?.t !== "compensate") {
          throw new Error(
            `compensate query fatal error: ctimeout not found at index ${index}`
          );
        }

        const compensationIndex = index;
        const antiIndex = index + ctimeout.antiIndexOffset;
        const firstCompensationIndex = index + ctimeout.withIndexOffset + 1;
        const firstCompensation = workflow.at(firstCompensationIndex);
        if (firstCompensation?.t !== "event") {
          throw new Error(
            `compensate query fatal error: compensation's first code is not of type event`
          );
        }

        return {
          compensationIndex,
          ctimeout,
          firstCompensation,
          firstCompensationIndex,
          antiTimeoutIndex: antiIndex,
        };
      })
      .sort((a, b) => {
        // in case of nested timeouts: sort by first/deepest-most timeout
        return a.antiTimeoutIndex - b.antiTimeoutIndex;
      });

  /**
   * extract available commands, recursesively when needed
   */
  const extractAvailableCommandFromSingularCode = (
    result: ReturnType<WFMachine<CType>["availableCommands"]>,
    code: CItem<CType>
  ) => {
    if (code?.t === "par") {
      const { atStack, maxReached, minReached } = fetchParallelCriteria(
        { index: data.executionIndex },
        code
      );

      if (!maxReached) {
        const eventCode = workflow.at(
          data.executionIndex + code.firstEventIndex
        );
        if (eventCode) {
          extractAvailableCommandFromSingularCode(result, eventCode);
        }
      }

      atStack.instances.map((instance) => {
        const eventCode = workflow.at(
          data.executionIndex + code.firstEventIndex + instance.entry.length
        );
        if (eventCode) {
          extractAvailableCommandFromSingularCode(result, eventCode);
        }
      });

      if (minReached) {
        const maybeCEv = workflow.at(atStack.nextEvalIndex);
        if (maybeCEv) {
          extractAvailableCommandFromSingularCode(result, maybeCEv);
        }
      }
    }

    if (code?.t === "match") {
      const smatch = data.stack.at(data.executionIndex);
      if (smatch && smatch.t === "match") {
        result.push(...smatch.inner.availableCommands());
      }
    }

    if (code?.t === "event") {
      const { name, control } = code;
      result.push({ name, role: code.role, control, reason: null });
    }

    if (code?.t === "choice") {
      const eventStartIndex = data.executionIndex + 1;
      const antiIndex = data.executionIndex + code.antiIndexOffset;
      workflow
        .slice(eventStartIndex, antiIndex) // take the CEvent between the choice and anti-choice
        .forEach((x) => {
          if (x.t !== "event") {
            // defensive measure, should not exist
            throw new Event("codes inside are not CEvent");
          }

          extractAvailableCommandFromSingularCode(result, x);
        });
    }

    availableTimeout().forEach((timeout) => {
      extractAvailableCommandFromSingularCode(
        result,
        timeout.ctimeout.consequence
      );
    });

    availableCompensations().forEach((compensation) => {
      extractAvailableCommandFromSingularCode(
        result,
        compensation.firstCompensation
      );
    });
  };

  const availableCommands = (): ReturnType<
    WFMachine<CType>["availableCommands"]
  > => {
    const code = workflow.at(data.executionIndex);
    const result: ReturnType<WFMachine<CType>["availableCommands"]> = [];

    if (code) {
      extractAvailableCommandFromSingularCode(result, code);
    }

    return result;
  };

  const fetchParallelCriteria = (
    evalContext: EvalContext,
    parallelCode: CParallel
  ) => {
    const {
      count: { max, min },
      pairOffsetIndex,
      firstEventIndex,
    } = parallelCode;

    const minCriteria = Math.max(min !== undefined ? min : 0, 0);
    const maxCriteria = Math.min(max !== undefined ? max : Infinity, Infinity);

    const atStack = ((): SParallel<CType> => {
      const atStack = data.stack.at(evalContext.index);
      if (!atStack) {
        const newAtStack: SParallel<CType> = {
          t: "par",
          fulfilled: false,
          instances: [],
          nextEvalIndex: evalContext.index + pairOffsetIndex + 1,
        };
        data.stack[evalContext.index] = newAtStack;
        return newAtStack;
      }
      if (atStack.t !== "par") {
        throw new Error("stack type not par");
      }
      return atStack;
    })();

    const execDoneCount = atStack.instances.filter(
      (instance) =>
        evalContext.index + firstEventIndex + instance.entry.length >=
        pairOffsetIndex
    ).length;

    const maxReached = execDoneCount >= maxCriteria;
    const minReached = execDoneCount >= minCriteria;

    return { atStack, maxReached, minReached };
  };

  const recalculatePar = (stackItem: SParallel<CType>) => {
    data.returnValue = [
      Parallel,
      stackItem.instances.map(
        (instance) => instance.entry[instance.entry.length - 1]?.name
      ),
    ];
  };

  /**
   * Catch up with execution index
   */
  const recalculateResult = (evalContext: EvalContext) => {
    while (data.resultCalcIndex < evalContext.index) {
      const code = workflow.at(data.resultCalcIndex);
      const stackItem = data.stack.at(data.resultCalcIndex);
      if (code?.t === "event" && stackItem?.t === "event") {
        code.bindings?.forEach((x) => {
          data.context[x.var] = stackItem.payload[x.index];
        });
        data.returnValue = [One, code.name];

        if (code.control === "return") {
          data.returned = true;
        }
      }

      if (code?.t === "anti-timeout" && stackItem?.t === "anti-timeout") {
        const consequenceData = stackItem.data;
        data.returnValue = [One, consequenceData.name];

        const consequence = stackItem.consequence;
        if (consequence.control === Code.Control.fail) {
          const retryIndex = findRetryOnStack(data.resultCalcIndex);
          if (retryIndex === null) {
            throw new Error("cannot find retry while dealing with ");
          }
          resetIndex(evalContext, retryIndex + 1);
          continue; // important
        } else if (consequence.control === "return") {
          data.returned = true;
        }
      }

      if (code?.t === "par" && stackItem?.t === "par") {
        recalculatePar(stackItem);
      }

      data.resultCalcIndex += 1;
    }

    // Handle parallel code
    const code = workflow.at(data.resultCalcIndex);
    const stackItem = data.stack.at(data.resultCalcIndex);
    if (code?.t === "par" && stackItem?.t === "par") {
      recalculatePar(stackItem);
    }
  };

  type Continue = boolean;
  type EvalContext = { index: number };
  const evaluateImpl = (evalContext: EvalContext): Continue => {
    // Handle Retry Code
    const code = workflow.at(evalContext.index);
    if (!code) {
      data.returned = true;
      return false;
    }

    if (code.t === "retry") {
      data.stack[evalContext.index] = { t: "retry" };
      evalContext.index += 1;
      return true;
    }

    if (code.t === "anti-retry") {
      const matchingRetryIndex = findMatchingRetryIndex(
        code,
        evalContext.index
      );
      if (typeof matchingRetryIndex !== "number") {
        throw new Error("retry not found");
      }
      data.stack[matchingRetryIndex] = null;
      evalContext.index += 1;
      return true;
    }

    // Handle timeout code

    if (code.t === "timeout") {
      const pair = findMatchingAntiTimeout(code, evalContext.index);
      if (typeof pair !== "number") {
        throw new Error("anti-timeout not found");
      }
      data.stack[evalContext.index] = {
        t: "timeout",
        startedAt: new Date(),
      };
      data.activeTimeout.add(evalContext.index);
      evalContext.index += 1;
      return true;
    }

    if (code.t === "anti-timeout") {
      nullifyMatchingTimeout(code, evalContext.index);
      evalContext.index += 1;
      return true;
    }

    if (code.t === "compensate") {
      data.activeCompensation.add(evalContext.index);
      evalContext.index += 1;
      return true;
    }

    if (code.t === "compensate-with") {
      const compensateIndex = evalContext.index + code.baseIndexOffset;
      const rightAfterAntiIndex = evalContext.index + code.antiIndexOffset + 1;
      data.activeCompensation.delete(compensateIndex);
      evalContext.index = rightAfterAntiIndex;
      return true;
    }

    if (code.t === "anti-compensate") {
      const compensateIndex = evalContext.index + code.baseIndexOffset;
      data.activeCompensation.delete(compensateIndex);
      // NOTE: do nothing else now
      // we might need to put a marker in the "compensate"'s stack counterpart
      evalContext.index += 1;
      return true;
    }

    if (code.t === "anti-choice") {
      // pointing the index into the anti-choice before the next code is
      // important because it allows the machine to process a new state
      // recalculation up to this point which may result in a return or a fail,
      // without continuing with other codes which may be automatic (e.g. retry, timeout, etc)
      evalContext.index += 1;
      return true;
    }

    if (code.t === "match") {
      const atStack = getSMatchAtIndex(evalContext.index) || {
        t: "match",
        inner: WFMachine<CType>(code.subworkflow),
      };
      data.stack[evalContext.index] = atStack;
      if (!atStack.inner.returned()) return false;

      // calculate returned
      const { state } = atStack.inner.state();
      const oneStateOrNull = (() => {
        if (state === null) return null;
        if (state[0] === One) return state[1];
        throw new Error("submachine returns parallel, which is invalid");
      })();

      const firstMatch = code.casesIndexOffsets
        .map((offset) => {
          const index = evalContext.index + offset;
          const matchCase = workflow.at(index);
          if (matchCase?.t !== "match-case") {
            throw new Error(
              `case index offset points to the wrong code type: ${matchCase?.t}`
            );
          }

          return { offset, matchCase };
        })
        .find(
          (x) =>
            x.matchCase.case[0] === Otherwise ||
            (x.matchCase.case[0] === Name &&
              oneStateOrNull &&
              x.matchCase.case[1] === oneStateOrNull)
        );

      if (!firstMatch) {
        throw new Error(`no case matches for ${oneStateOrNull} at ${code}`);
      }

      evalContext.index += firstMatch.offset + 1;
      return true;
    }

    if (code.t === "anti-match-case") {
      evalContext.index += code.afterIndexOffset;
      return true;
    }

    return false;
  };

  const evaluate = (evalContext: EvalContext) => {
    while (true) {
      recalculateResult(evalContext);
      if (data.returned) break;

      const shouldContinue = evaluateImpl(evalContext);

      if (shouldContinue) continue;

      break;
    }
  };

  const feedCompensation = (evalContext: EvalContext, e: EEvent<CType>) => {
    const firstMatching = availableCompensations()
      .filter((x) => x.firstCompensation.name === e.name)
      .at(0);

    if (firstMatching) {
      const firstMatchingIndex = firstMatching.firstCompensationIndex;
      data.activeCompensation.delete(firstMatching.compensationIndex);
      evalContext.index = firstMatchingIndex;
      data.executionIndex = firstMatchingIndex;
      return feedEvent(evalContext, firstMatching.firstCompensation, e);
    }
    return false;
  };

  const feedTimeout = (evalContext: EvalContext, e: EEvent<CType>) => {
    const lastMatching = availableTimeout()
      .filter((x) => x.ctimeout.consequence.name === e.name)
      .at(0);

    if (lastMatching) {
      data.stack[lastMatching.antiIndex] = {
        t: "anti-timeout",
        consequence: lastMatching.ctimeout.consequence,
        data: {
          t: "event",
          name: lastMatching.ctimeout.consequence.name,
          payload: {},
        },
      };

      evalContext.index = lastMatching.antiIndex + 1;
      data.executionIndex = lastMatching.antiIndex + 1;

      return true;
    }
    return false;
  };

  const feedEvent = (
    evalContext: EvalContext,
    code: CEvent<CType>,
    e: EEvent<CType>
  ) => {
    if (e.name === code.name) {
      data.stack[evalContext.index] = {
        t: "event",
        payload: e.payload,
      };
      evalContext.index += 1;
      return true;
    }
    return false;
  };

  const feedChoice = (
    evalContext: EvalContext,
    code: CChoice,
    e: EEvent<CType>
  ) => {
    const eventStartIndex = evalContext.index + 1;
    const antiIndex = evalContext.index + code.antiIndexOffset;
    const firstMatching = workflow
      .slice(eventStartIndex, antiIndex) // take the CEvent between the choice and anti-choice
      .map((x, index) => {
        if (x.t !== "event") {
          // defensive measure, should not exist
          throw new Event("codes inside are not CEvent");
        }
        return [index, x] as const;
      })
      .find(([_, x]) => x.name === e.name);

    if (firstMatching) {
      const [indexOffset, _] = firstMatching;
      const eventIndex = eventStartIndex + indexOffset;
      data.stack[eventIndex] = {
        t: "event",
        payload: e.payload,
      };
      evalContext.index = antiIndex;
      return true;
    }
    return false;
  };

  const feed = (
    evalContext: EvalContext,
    code: CItem<CType>,
    e: EEvent<CType>
  ) => {
    let isFed = false;

    if (code?.t === "event") {
      isFed = feedEvent(evalContext, code, e);
    }

    if (!isFed && code?.t === "choice") {
      isFed = feedChoice(evalContext, code, e);
    }

    if (!isFed) {
      isFed = feedTimeout(evalContext, e);
    }

    if (!isFed) {
      isFed = feedCompensation(evalContext, e);
    }
    return isFed;
  };

  const tickInnerParallelRecursive = (
    evalContext: EvalContext,
    parallelCode: CParallel,
    e: EEvent<CType> | null
  ): { nextEvaluated: null | number; fed: boolean } => {
    let nextEvaluated = null;
    let fed = false;
    const { maxReached, minReached, atStack } = fetchParallelCriteria(
      evalContext,
      parallelCode
    );

    // new instance
    if (e && !maxReached) {
      const eventCode = workflow.at(
        evalContext.index + parallelCode.firstEventIndex
      );
      if (eventCode?.t !== "event") {
        throw new Error("parallel.firstEventIndex is not event code");
      }

      if (eventCode.name === e.name) {
        const newInstance: SParallelExecution<CType> = { entry: [e] };
        atStack.instances.push(newInstance);
        fed = true;
      }
    }

    // instances resumption
    // TODO: this doesn't handle non-events on parallel
    if (e && !fed) {
      const firstMatching = atStack.instances
        .map(
          (instance) =>
            [
              instance,
              workflow.at(
                evalContext.index +
                  parallelCode.firstEventIndex +
                  instance.entry.length
              ),
            ] as const
        )
        .filter(
          (pair): pair is [SParallelExecution<CType>, CEvent<CType>] =>
            pair[1]?.t === "event" && pair[1]?.name === e.name
        )
        .at(0);

      if (firstMatching) {
        const [instance, _] = firstMatching;
        instance.entry.push(e);
        fed = true;
      }
    }

    if (minReached) {
      const nextEvalContext = { index: atStack.nextEvalIndex };
      // move nextEvalIndex as far as it can
      evaluate(nextEvalContext);

      const innerCode = workflow.at(nextEvalContext.index);
      if (!fed && innerCode && e) {
        fed =
          innerCode.t === "par"
            ? tickInnerParallelRecursive(nextEvalContext, innerCode, e).fed
            : feed(nextEvalContext, innerCode, e);

        if (fed) {
          nextEvaluated = nextEvalContext.index;
        }
      }

      atStack.nextEvalIndex = nextEvalContext.index;
    }

    if (maxReached) {
      nextEvaluated = Math.max(
        atStack.nextEvalIndex,
        evalContext.index + parallelCode.pairOffsetIndex + 1
      );
    }

    return {
      fed,
      nextEvaluated,
    };
  };

  const tickParallel = (
    parallelCode: CParallel,
    e: EEvent<CType> | null
  ): boolean => {
    let evalContext = { index: data.executionIndex };
    const tickParallelRes = tickInnerParallelRecursive(
      evalContext,
      parallelCode,
      e
    );
    if (tickParallelRes.nextEvaluated !== null) {
      data.executionIndex = tickParallelRes.nextEvaluated;
    }

    evalContext = { index: data.executionIndex };
    evaluate(evalContext);
    data.executionIndex = evalContext.index;

    return tickParallelRes.fed;
  };

  const tickMatch = (_: CMatch<CType>, e: EEvent<CType> | null): boolean => {
    const evalContext = { index: data.executionIndex };
    const atStack = getSMatchAtIndex(evalContext.index);
    if (!atStack) {
      throw new Error("missing match at stack on evaluation");
    }
    data.stack[evalContext.index] = atStack;
    const res = atStack.inner.tick(e);

    evaluate(evalContext);
    data.executionIndex = evalContext.index;
    return res;
  };

  const tick = (e: EEvent<CType> | null): boolean => {
    const evalContext = { index: data.executionIndex };
    const code = workflow.at(evalContext.index);
    let fed = false;
    if (code?.t === "match") {
      return tickMatch(code, e);
    }

    if (code?.t === "par") {
      return tickParallel(code, e);
    }

    if (code && e) {
      fed = feed(evalContext, code, e);
    }

    evaluate(evalContext);
    data.executionIndex = evalContext.index;
    return fed;
  };

  const state = () => ({
    state: data.returnValue,
    context: data.context,
  });

  const returned = () => data.returned;

  const availableTimeoutExternal = () =>
    availableTimeout().map(
      ({
        ctimeout: {
          consequence: { name, control },
        },
        lateness,
      }) => ({
        consequence: { name, control },
        dueFor: lateness,
      })
    );

  return {
    tick,
    state,
    returned,
    availableTimeout: availableTimeoutExternal,
    availableCompensations: () =>
      availableCompensations().map(({ firstCompensation }) => ({
        name: firstCompensation.name,
      })),
    availableCommands,
  };
};

type TheType = makeCType<{ role: "a"; ev: Ev }>;

describe("enums", () => {
  it("enums", () => {
    expect(Enum(["a", "b", "c"])).toEqual({
      a: "a",
      b: "b",
      c: "c",
    });
  });
});

describe("machine", () => {
  describe("event", () => {
    it("event", () => {
      const code = Code.make<TheType>();
      const machine = WFMachine<TheType>([
        code.event("a", Ev.request, {
          bindings: [code.binding("src", "from"), code.binding("dst", "to")],
        }),
      ]);

      expect(machine.returned()).toBe(false);

      machine.tick(
        Emit.event(Ev.request, { from: "storage-1", to: "storage-2" })
      );

      expect(machine.returned()).toBe(true);
      expect(machine.state()).toEqual({
        state: [One, Ev.request],
        context: { src: "storage-1", dst: "storage-2" },
      });
    });
  });

  describe("retry-timeout", () => {
    it("retry-timeout FAIL", async () => {
      const TIMEOUT_DURATION = 300;

      const code = Code.make<TheType>();

      const machine = WFMachine<TheType>([
        code.event("a", Ev.request, {
          bindings: [code.binding("src", "from"), code.binding("dst", "to")],
        }),
        ...code.retry([
          code.event("a", Ev.reqStorage, {
            bindings: [code.binding("somevar", "somefield")],
          }),
          ...code.timeout(
            TIMEOUT_DURATION,
            [code.event("a", Ev.bid)],
            code.event("a", Ev.cancelled, {
              control: Code.Control.fail,
            })
          ),
        ]),
      ]);

      expect(machine.returned()).toBe(false);
      machine.tick(
        Emit.event(Ev.request, { from: "storage-1", to: "storage-2" })
      );
      expect(machine.returned()).toBe(false);

      machine.tick(Emit.event(Ev.reqStorage, { somefield: "somevalue" }));
      expect(machine.returned()).toBe(false);
      expect(machine.state()).toEqual({
        state: [One, Ev.reqStorage],
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });
      expect(machine.availableTimeout()).toEqual([]);

      // attempt timeout will fail
      machine.tick(Emit.event(Ev.cancelled, {}));
      expect(machine.returned()).toBe(false);
      expect(machine.state()).toEqual({
        state: [One, Ev.reqStorage],
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });

      // after some moments some timeouts are available
      await sleep(TIMEOUT_DURATION + 100);
      expect(
        machine
          .availableTimeout()
          .findIndex(
            ({ consequence: { name, control } }) =>
              name === Ev.cancelled && control === Code.Control.fail
          ) !== -1
      ).toBe(true);

      // trigger timeout - state will be wound back to when RETRY
      machine.tick(Emit.event(Ev.cancelled, {}));
      expect(machine.returned()).toBe(false);
      expect(machine.state()).toEqual({
        state: [One, Ev.request],
        context: { src: "storage-1", dst: "storage-2" },
      });
    });

    it("retry-timeout RETURN", async () => {
      const TIMEOUT_DURATION = 300;
      const code = Code.make<TheType>();

      const machine = WFMachine<TheType>([
        code.event("a", Ev.request, {
          bindings: [code.binding("src", "from"), code.binding("dst", "to")],
        }),
        ...code.retry([
          code.event("a", Ev.reqStorage, {
            bindings: [code.binding("somevar", "somefield")],
          }),
          ...code.timeout(
            TIMEOUT_DURATION,
            [code.event("a", Ev.bid)],
            code.event("a", Ev.cancelled, { control: Code.Control.return })
          ),
        ]),
      ]);

      expect(machine.returned()).toBe(false);
      machine.tick(
        Emit.event(Ev.request, { from: "storage-1", to: "storage-2" })
      );
      expect(machine.returned()).toBe(false);

      machine.tick(Emit.event(Ev.reqStorage, { somefield: "somevalue" }));
      expect(machine.returned()).toBe(false);
      expect(machine.state()).toEqual({
        state: [One, Ev.reqStorage],
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });

      await sleep(TIMEOUT_DURATION + 100);
      // trigger timeout - also triggering return
      machine.tick(Emit.event(Ev.cancelled, {}));
      expect(machine.returned()).toBe(true);
      expect(machine.state()).toEqual({
        state: [One, Ev.cancelled],
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });
    });

    it("retry-timeout PASS", async () => {
      const TIMEOUT_DURATION = 300;
      const code = Code.make<TheType>();

      const machine = WFMachine<TheType>([
        code.event("a", Ev.request, {
          bindings: [code.binding("src", "from"), code.binding("dst", "to")],
        }),
        ...code.retry([
          code.event("a", Ev.reqStorage, {
            bindings: [code.binding("somevar", "somefield")],
          }),
          ...code.timeout(
            TIMEOUT_DURATION,
            [code.event("a", Ev.bid)],
            code.event("a", Ev.cancelled, { control: Code.Control.fail })
          ),
        ]),
      ]);

      expect(machine.returned()).toBe(false);
      machine.tick(
        Emit.event(Ev.request, { from: "storage-1", to: "storage-2" })
      );
      expect(machine.returned()).toBe(false);

      machine.tick(Emit.event(Ev.reqStorage, { somefield: "somevalue" }));
      expect(machine.returned()).toBe(false);
      expect(machine.state()).toEqual({
        state: [One, Ev.reqStorage],
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });

      machine.tick(Emit.event(Ev.bid, {}));

      await sleep(TIMEOUT_DURATION + 100);
      machine.tick(null);
      expect(machine.state()).toEqual({
        state: [One, Ev.bid],
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });
    });
  });

  describe("parallel", () => {
    it("produce parallel state and work the next workable code and event", () => {
      const code = Code.make<TheType>();
      const machine = WFMachine<TheType>([
        code.event("a", Ev.request),
        ...code.parallel({ min: 2 }, [code.event("a", Ev.bid)]), // minimum of two bids
        ...code.retry([code.event("a", Ev.accept)]), // test that next code is not immediately event too
      ]);

      machine.tick(Emit.event(Ev.request, {}));
      expect(machine.state()).toEqual({
        state: [One, Ev.request],
        context: {},
      });

      machine.tick(Emit.event(Ev.bid, {}));
      expect(machine.state()).toEqual({
        state: [Parallel, [Ev.bid]],
        context: {},
      });

      machine.tick(Emit.event(Ev.accept, {})); // attempt to accept will fail because parallel count isn't fulfilled
      expect(machine.state()).toEqual({
        state: [Parallel, [Ev.bid]],
        context: {},
      });

      machine.tick(Emit.event(Ev.bid, {})); // the second bid
      expect(machine.state()).toEqual({
        state: [Parallel, [Ev.bid, Ev.bid]],
        context: {},
      });

      machine.tick(Emit.event(Ev.accept, {})); // finally accept should work
      expect(machine.state()).toEqual({
        state: [One, Ev.accept],
        context: {},
      });
    });

    it("works with choice", () => {
      const code = Code.make<TheType>();
      const machine = WFMachine<TheType>([
        code.event("a", Ev.request),
        ...code.parallel({ min: 0 }, [code.event("a", Ev.bid)]), // minimum of two bids
        ...code.choice([code.event("a", Ev.accept), code.event("a", Ev.deny)]),
      ]);

      machine.tick(Emit.event(Ev.request, {}));
      machine.tick(Emit.event(Ev.deny, {}));
      expect(machine.state()).toEqual({
        state: [One, Ev.deny],
        context: {},
      });
    });

    it("sequence of parallels", () => {
      const code = Code.make<TheType>();
      const machine = WFMachine<TheType>([
        code.event("a", Ev.request),
        ...code.parallel({ min: 2 }, [code.event("a", Ev.bid)]), // minimum of two bids
        ...code.parallel({ min: 2 }, [code.event("a", Ev.accept)]), // minimum of two bids
      ]);

      machine.tick(Emit.event(Ev.request, {}));
      machine.tick(Emit.event(Ev.bid, {}));
      expect(machine.state().state).toEqual([Parallel, [Ev.bid]]);
      machine.tick(Emit.event(Ev.bid, {}));
      expect(machine.state().state).toEqual([Parallel, [Ev.bid, Ev.bid]]);
      machine.tick(Emit.event(Ev.accept, {}));
      expect(machine.state().state).toEqual([Parallel, [Ev.accept]]);
      machine.tick(Emit.event(Ev.accept, {}));
      expect(machine.state().state).toEqual([Parallel, [Ev.accept, Ev.accept]]);
    });

    it("max reached force next", () => {
      const code = Code.make<TheType>();
      const machine = WFMachine<TheType>([
        code.event("a", Ev.request),
        ...code.parallel({ min: 1, max: 2 }, [code.event("a", Ev.bid)]), // minimum of two bids
      ]);

      machine.tick(Emit.event(Ev.request, {}));
      machine.tick(Emit.event(Ev.bid, {}));
      expect(machine.state().state).toEqual([Parallel, [Ev.bid]]);
      machine.tick(Emit.event(Ev.bid, {}));
      expect(machine.state().state).toEqual([Parallel, [Ev.bid, Ev.bid]]);
      expect(machine.returned()).toBe(true);
    });
  });

  describe("compensation", () => {
    it("passing without compensation", () => {
      const code = Code.make<TheType>();
      const machine = WFMachine<TheType>([
        code.event("a", Ev.inside, {}),
        ...code.compensate(
          [
            code.event("a", Ev.reqLeave),
            code.event("a", Ev.doLeave),
            code.event("a", Ev.success),
          ],
          [
            code.event("a", Ev.withdraw),
            code.event("a", Ev.doLeave),
            code.event("a", Ev.withdrawn),
          ]
        ),
      ]);

      machine.tick(Emit.event(Ev.inside, {}));
      expect(machine.state().state).toEqual([One, Ev.inside]);

      machine.tick(Emit.event(Ev.reqLeave, {}));
      expect(machine.state().state).toEqual([One, Ev.reqLeave]);
      expect(machine.availableCompensations()).toEqual(
        [Ev.withdraw].map((name) => ({ name }))
      );

      machine.tick(Emit.event(Ev.doLeave, {}));
      expect(machine.state().state).toEqual([One, Ev.doLeave]);
      expect(machine.availableCompensations()).toEqual(
        [Ev.withdraw].map((name) => ({ name }))
      );

      machine.tick(Emit.event(Ev.success, {}));
      expect(machine.state().state).toEqual([One, Ev.success]);
      expect(machine.availableCompensations()).toEqual([]);
      expect(machine.returned()).toEqual(true);
    });

    it("passing with compensation", () => {
      const code = Code.make<TheType>();
      const machine = WFMachine<TheType>([
        code.event("a", Ev.inside, {}),
        ...code.compensate(
          [
            code.event("a", Ev.reqLeave),
            code.event("a", Ev.doLeave),
            code.event("a", Ev.success),
          ],
          [
            code.event("a", Ev.withdraw),
            code.event("a", Ev.doLeave),
            code.event("a", Ev.withdrawn),
          ]
        ),
      ]);

      machine.tick(Emit.event(Ev.inside, {}));
      expect(machine.state().state).toEqual([One, Ev.inside]);

      machine.tick(Emit.event(Ev.reqLeave, {}));
      expect(machine.state().state).toEqual([One, Ev.reqLeave]);
      expect(machine.availableCompensations()).toEqual(
        [Ev.withdraw].map((name) => ({ name }))
      );

      machine.tick(Emit.event(Ev.withdraw, {}));
      expect(machine.state().state).toEqual([One, Ev.withdraw]);
      expect(machine.availableCompensations()).toEqual([]);
      machine.tick(Emit.event(Ev.doLeave, {}));
      expect(machine.state().state).toEqual([One, Ev.doLeave]);
      machine.tick(Emit.event(Ev.withdrawn, {}));
      expect(machine.state().state).toEqual([One, Ev.withdrawn]);
      expect(machine.returned()).toEqual(true);
    });
  });

  describe("match", () => {
    it("named match should work", () => {
      const code = Code.make<TheType>();
      const machine = WFMachine<TheType>([
        code.event("a", Ev.request),
        ...code.match(
          [
            code.event("a", Ev.inside),
            ...code.compensate(
              [
                code.event("a", Ev.reqLeave),
                code.event("a", Ev.doLeave),
                code.event("a", Ev.success),
              ],
              [
                code.event("a", Ev.withdraw),
                code.event("a", Ev.doLeave),
                code.event("a", Ev.withdrawn),
              ]
            ),
          ],
          [
            code.matchCase([Name, Ev.success], []),
            code.matchCase(
              [Otherwise],
              [code.event("a", Ev.cancelled, { control: Code.Control.return })]
            ),
          ]
        ),
        code.event("a", Ev.done),
      ]);

      machine.tick(Emit.event(Ev.request, {}));
      machine.tick(Emit.event(Ev.inside, {}));
      machine.tick(Emit.event(Ev.reqLeave, {}));
      machine.tick(Emit.event(Ev.doLeave, {}));
      machine.tick(Emit.event(Ev.success, {}));
      machine.tick(Emit.event(Ev.done, {}));
      expect(machine.returned()).toBe(true);
      expect(machine.state().state).toEqual([One, Ev.done]);
    });

    it("otherwise match should work", () => {
      const code = Code.make<TheType>();
      const machine = WFMachine<TheType>([
        code.event("a", Ev.request),
        ...code.match(
          [
            code.event("a", Ev.inside),
            ...code.compensate(
              [
                code.event("a", Ev.reqLeave),
                code.event("a", Ev.doLeave),
                code.event("a", Ev.success),
              ],
              [
                code.event("a", Ev.withdraw),
                code.event("a", Ev.doLeave),
                code.event("a", Ev.withdrawn),
              ]
            ),
          ],
          [
            code.matchCase([Name, Ev.success], []),
            code.matchCase(
              [Otherwise],
              [code.event("a", Ev.cancelled, { control: Code.Control.return })]
            ),
          ]
        ),
        code.event("a", Ev.done),
      ]);

      machine.tick(Emit.event(Ev.request, {}));
      machine.tick(Emit.event(Ev.inside, {}));
      machine.tick(Emit.event(Ev.reqLeave, {}));
      machine.tick(Emit.event(Ev.doLeave, {}));
      machine.tick(Emit.event(Ev.withdraw, {}));
      machine.tick(Emit.event(Ev.doLeave, {}));
      machine.tick(Emit.event(Ev.withdrawn, {}));
      machine.tick(Emit.event(Ev.cancelled, {}));
      expect(machine.returned()).toBe(true);
      expect(machine.state().state).toEqual([One, Ev.cancelled]);
    });
  });

  describe("choice", () => {
    const code = Code.make<TheType>();
    const prepare = () =>
      WFMachine<TheType>([
        code.event("a", Ev.request),
        ...code.choice([
          code.event("a", Ev.accept),
          code.event("a", Ev.deny, { control: Code.Control.return }),
          code.event("a", Ev.assistanceNeeded, {
            control: Code.Control.return,
          }),
        ]),
        code.event("a", Ev.doEnter),
      ]);

    it("should work for any event inside the choice - 1", () => {
      const machine = prepare();
      machine.tick(Emit.event(Ev.request, {}));
      machine.tick(Emit.event(Ev.accept, {}));
      expect(machine.returned()).toEqual(false);
      expect(machine.state().state).toEqual([One, Ev.accept]);
      machine.tick(Emit.event(Ev.doEnter, {}));
      expect(machine.returned()).toEqual(true);
      expect(machine.state().state).toEqual([One, Ev.doEnter]);
    });

    it("should work for any event inside the choice - 2", () => {
      const machine = prepare();
      machine.tick(Emit.event(Ev.request, {}));
      machine.tick(Emit.event(Ev.deny, {}));
      expect(machine.returned()).toEqual(true);
      expect(machine.state().state).toEqual([One, Ev.deny]);
    });

    it("should work for any event inside the choice - 3", () => {
      const machine = prepare();
      machine.tick(Emit.event(Ev.request, {}));
      machine.tick(Emit.event(Ev.assistanceNeeded, {}));
      expect(machine.returned()).toEqual(true);
      expect(machine.state().state).toEqual([One, Ev.assistanceNeeded]);
    });
  });
});
