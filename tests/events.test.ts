import { describe, expect, it } from "@jest/globals";
import { v4 as uuidv4 } from "uuid";

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

type CItem =
  | CAnti
  | CEvent
  | CRetry
  | CTimeout
  | CParallel
  | CCompensate
  | CCompensateWith
  | CMatch
  | CMatchCase;
type CAnti =
  | CAntiRetry
  | CAntiTimeout
  | CAntiParallel
  | CAntiCompensate
  | CAntiMatchCase;

type CEventBinding = { var: string; index: string };
type CEvent = {
  t: "event";
  name: Ev;
  bindings?: CEventBinding[];
  control?: Code.Control;
};
type CMatch = {
  t: "match";
  subworkflow: readonly [CEvent, ...CItem[]];
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
type CTimeout = {
  t: "timeout";
  duration: number;
  consequence: CEvent;
  pairOffsetIndex: number;
};

type CAntiRetry = { t: "anti-retry"; pairOffsetIndex: number };
type CAntiTimeout = { t: "anti-timeout"; pairOffsetIndex: number };
type CAntiParallel = { t: "anti-par"; pairOffsetIndex: number };

namespace Code {
  export const Control = Enum(["fail", "return"] as const);
  export type Control = Enum<typeof Control>;

  export const binding = (name: string, index: string): CEventBinding => ({
    index,
    var: name,
  });

  export const event = (
    name: Ev,
    x?: Pick<CEvent, "bindings" | "control">
  ): CEvent => ({ t: "event", name, ...(x || {}) });

  export const retry = (workflow: CItem[]): CItem[] => [
    { t: "retry", pairOffsetIndex: workflow.length + 1 },
    ...workflow,
    { t: "anti-retry", pairOffsetIndex: (workflow.length + 1) * -1 },
  ];

  export const compensate = (
    main: [CEvent, ...CItem[]],
    compensation: [CEvent, ...CItem[]]
  ): CItem[] => {
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

  export const match = (
    workflow: readonly [CEvent, ...CItem[]],
    cases: [CMatchCaseType, readonly CItem[]][]
  ): CItem[] => {
    let index = 0;
    const inlinedCases: CItem[] = [];
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

  export const parallel = (
    count: CParallel["count"],
    workflow: CEvent[]
  ): CItem[] => [
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

  export const timeout = (
    duration: number,
    workflow: CItem[],
    consequence: CEvent
  ): CItem[] => [
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
type StackItem =
  | SMatch
  | SEvent
  | SRetry
  | STimeout
  | SAntiTimeout
  | SParallel
  | SAntiParallel;

type SMatch = Pick<CMatch, "t"> & {
  inner: WFMachine;
};
type SEvent = CEvent & { payload: EEvent["payload"] };
type SRetry = Pick<CRetry, "t">;
type SParallelExecution = { entry: EEvent[] };
type SParallel = Pick<CParallel, "t"> & {
  fulfilled: boolean;
  nextEvalIndex: number;
  instances: SParallelExecution[];
};
type STimeout = Pick<CTimeout, "t"> & {
  startedAt: Date;
};
type SAntiTimeout = Pick<CAntiTimeout, "t"> & {
  consequence: CTimeout["consequence"];
  data: EEvent;
};
type SAntiParallel = Pick<CAntiParallel, "t"> & {};

// Payload

type EEvent = { t: "event"; name: Ev; payload: Record<string, unknown> };
namespace Emit {
  export const event = (
    name: Ev,
    payload: Record<string, unknown>
  ): EEvent => ({
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

type WFMachine = {
  tick: (state: EEvent | null) => void;
  state: () => { state: State<Ev> | null; context: Record<string, unknown> };
  returned: () => boolean;
  availableTimeout: () => {
    consequence: {
      name: Ev;
      control: Code.Control | undefined;
    };
    dueFor: number;
  }[];
  availableCompensations: () => {
    name: Ev;
  }[];
};

const WFMachine = (workflow: Readonly<[CEvent, ...CItem[]]>): WFMachine => {
  const data = {
    executionIndex: 0,
    stack: [] as (StackItem | null)[],

    activeTimeout: new Set() as Set<number>,
    activeCompensation: new Set() as Set<number>,

    resultCalcIndex: 0,
    context: {} as Record<string, unknown>,
    returnValue: null as State<Ev> | null,
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
        data.returnValue = [One, stackItem.name];

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

      data.resultCalcIndex += 1;
    }

    // Handle parallel code
    const code = workflow.at(data.resultCalcIndex);
    const stack = data.stack.at(data.resultCalcIndex);
    if (code?.t === "par" && stack?.t === "par") {
      data.returnValue = [
        Parallel,
        stack.instances.map(
          (instance) => instance.entry[instance.entry.length - 1]?.name
        ),
      ];
    }
  };

  // Finders helpers

  const findMatchingAntiTimeout = (timeout: CTimeout, currentIndex: number) => {
    const pairOffset = timeout.pairOffsetIndex;
    const pairIndex = currentIndex + pairOffset;
    const code = workflow.at(pairIndex);
    if (code?.t === "anti-timeout") {
      return pairIndex;
    }
    return null;
  };

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

  // Helpers

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

    if (code.t === "match") {
      const atStack = getMatchAtIndex(evalContext.index);
      if (!atStack?.inner) {
        throw new Error("missing match at stack on evaluation");
      }
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

      evalContext.index = firstMatch.offset + 1;
      return true;
    }

    if (code.t === "anti-match-case") {
      evalContext.index = code.afterIndexOffset;
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

  const feedCompensation = (evalContext: EvalContext, e: EEvent) => {
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

  const feedTimeout = (evalContext: EvalContext, e: EEvent) => {
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

  const feedEvent = (evalContext: EvalContext, code: CEvent, e: EEvent) => {
    if (e.name === code.name) {
      data.stack[evalContext.index] = {
        t: "event",
        name: e.name,
        payload: e.payload,
        bindings: code.bindings,
      };
      evalContext.index += 1;
      return true;
    }
    return false;
  };

  const tickParallel = (parallelCode: CParallel, e: EEvent | null) => {
    let eIsFed = false;
    const atStack = ((): SParallel => {
      const atStack = data.stack.at(data.executionIndex);
      if (!atStack) {
        const newAtStack: SParallel = {
          t: "par",
          fulfilled: false,
          instances: [],
          nextEvalIndex: data.executionIndex + parallelCode.pairOffsetIndex + 1,
        };
        data.stack[data.executionIndex] = newAtStack;
        return newAtStack;
      }
      if (atStack.t !== "par") {
        throw new Error("stack type not par");
      }
      return atStack;
    })();

    if (e) {
      // new instance
      const eventCode = workflow.at(
        data.executionIndex + parallelCode.firstEventIndex
      );
      if (eventCode?.t !== "event") {
        throw new Error("parallel.firstEventIndex is not event code");
      }

      if (eventCode.name === e.name) {
        const newInstance: SParallelExecution = { entry: [e] };
        atStack.instances.push(newInstance);
        eIsFed = true;
      }

      // instances resumption
      if (!eIsFed) {
        const firstMatching = atStack.instances
          .map(
            (instance) =>
              [
                instance,
                workflow.at(
                  data.executionIndex +
                    parallelCode.firstEventIndex +
                    instance.entry.length
                ),
              ] as const
          )
          .filter(
            (pair): pair is [SParallelExecution, CEvent] =>
              pair[1]?.t === "event" && pair[1]?.name === e.name
          )
          .at(0);

        if (firstMatching) {
          const [instance, _] = firstMatching;
          instance.entry.push(e);
          eIsFed = true;
        }
      }
    }

    // fulfilled calculation
    const execDoneCount = atStack.instances.filter(
      (instance) =>
        data.executionIndex +
          parallelCode.firstEventIndex +
          instance.entry.length >=
        parallelCode.pairOffsetIndex
    ).length;
    const minCriteria = Math.max(parallelCode.count?.min || 1, 1);
    if (execDoneCount >= minCriteria) {
      atStack.fulfilled = true;
    }

    if (atStack.fulfilled) {
      const evalContext = { index: atStack.nextEvalIndex };
      const maybeEv = workflow.at(atStack.nextEvalIndex);
      const eventFed = (() => {
        if (!eIsFed && maybeEv?.t === "event" && e) {
          return feedEvent(evalContext, maybeEv, e);
        }
        return false;
      })();

      // if some next event is fed
      atStack.nextEvalIndex = evalContext.index;
      if (eventFed) {
        data.executionIndex = evalContext.index;
      }
    }

    const evalContext = { index: data.executionIndex };
    evaluate(evalContext);
    data.executionIndex = evalContext.index;
  };

  const getMatchAtIndex = (index: number) => {
    const atStack = data.stack.at(index);
    if (!atStack) {
      return null;
    }

    if (atStack.t !== "match") {
      throw new Error("match stack position filled with non-match");
    }

    return atStack;
  };

  const tickMatch = (matchCode: CMatch, e: EEvent | null) => {
    const evalContext = { index: data.executionIndex };
    const { inner } = getMatchAtIndex(evalContext.index) || {
      t: "match",
      inner: WFMachine(matchCode.subworkflow),
    };

    inner.tick(e);

    evaluate(evalContext);
    data.executionIndex = evalContext.index;
  };

  const tick = (e: EEvent | null) => {
    const evalContext = { index: data.executionIndex };
    const code = workflow.at(evalContext.index);
    if (code?.t === "match") {
      return tickMatch(code, e);
    }

    if (code?.t === "par") {
      return tickParallel(code, e);
    }

    if (e) {
      const isFed = (() => {
        if (code?.t === "event" && e) {
          return feedEvent(evalContext, code, e);
        }
        return false;
      })();

      if (!isFed) {
        feedTimeout(evalContext, e);
      }

      if (!isFed) {
        feedCompensation(evalContext, e);
      }
    }

    evaluate(evalContext);
    data.executionIndex = evalContext.index;
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
  };
};

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
  it("event", () => {
    const machine = WFMachine([
      Code.event(Ev.request, {
        bindings: [Code.binding("src", "from"), Code.binding("dst", "to")],
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

  describe("retry-timeout", () => {
    it("retry-timeout FAIL", async () => {
      const TIMEOUT_DURATION = 300;

      const machine = WFMachine([
        Code.event(Ev.request, {
          bindings: [Code.binding("src", "from"), Code.binding("dst", "to")],
        }),
        ...Code.retry([
          Code.event(Ev.reqStorage, {
            bindings: [Code.binding("somevar", "somefield")],
          }),
          ...Code.timeout(
            TIMEOUT_DURATION,
            [Code.event(Ev.bid)],
            Code.event(Ev.cancelled, { control: Code.Control.fail })
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

      const machine = WFMachine([
        Code.event(Ev.request, {
          bindings: [Code.binding("src", "from"), Code.binding("dst", "to")],
        }),
        ...Code.retry([
          Code.event(Ev.reqStorage, {
            bindings: [Code.binding("somevar", "somefield")],
          }),
          ...Code.timeout(
            TIMEOUT_DURATION,
            [Code.event(Ev.bid)],
            Code.event(Ev.cancelled, { control: Code.Control.return })
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

      const machine = WFMachine([
        Code.event(Ev.request, {
          bindings: [Code.binding("src", "from"), Code.binding("dst", "to")],
        }),
        ...Code.retry([
          Code.event(Ev.reqStorage, {
            bindings: [Code.binding("somevar", "somefield")],
          }),
          ...Code.timeout(
            TIMEOUT_DURATION,
            [Code.event(Ev.bid)],
            Code.event(Ev.cancelled, { control: Code.Control.fail })
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
    it("works", () => {
      const machine = WFMachine([
        Code.event(Ev.request),
        ...Code.parallel({ min: 2 }, [Code.event(Ev.bid)]), // minimum of two bids
        Code.event(Ev.accept),
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
  });

  describe("compensation", () => {
    it("passing without compensation", () => {
      const machine = WFMachine([
        Code.event(Ev.inside, {}),
        ...Code.compensate(
          [
            Code.event(Ev.reqLeave),
            Code.event(Ev.doLeave),
            Code.event(Ev.success),
          ],
          [
            Code.event(Ev.withdraw),
            Code.event(Ev.doLeave),
            Code.event(Ev.withdrawn),
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

    it("passing without compensation", () => {
      const machine = WFMachine([
        Code.event(Ev.inside, {}),
        ...Code.compensate(
          [
            Code.event(Ev.reqLeave),
            Code.event(Ev.doLeave),
            Code.event(Ev.success),
          ],
          [
            Code.event(Ev.withdraw),
            Code.event(Ev.doLeave),
            Code.event(Ev.withdrawn),
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
});
