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

type CItem = CAnti | CEvent | CRetry | CTimeout | CParallel; // | CAnti;
type CAnti = CAntiRetry | CAntiTimeout | CAntiParallel; //;

type CEventBinding = { var: string; index: string };
type CEvent = {
  t: "event";
  name: Ev;
  bindings?: CEventBinding[];
  control?: Code.Control;
};
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

  export const parallel = (
    workflow: CEvent[],
    count: CParallel["count"]
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
  | SEvent
  | SRetry
  | STimeout
  | SAntiTimeout
  | SParallel
  | SAntiParallel;
type SEvent = CEvent & { payload: EEvent["payload"] };
type SRetry = Pick<CRetry, "t">;
type SParallelExecution = { executionIndex: number };
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

const WFMachine = (workflow: [CEvent, ...CItem[]]) => {
  const data = {
    stack: [] as (StackItem | null)[],
    executionIndex: 0,
    contextCalculationIndex: 0,
    activeTimeout: new Set() as Set<number>,

    context: {} as Record<string, unknown>,
    returnValue: null as Ev | null,
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
    data.contextCalculationIndex = 0;
    data.context = {};
  };

  /**
   * Catch up with execution index
   */
  const recalculateResult = (evalContext: EvalContext) => {
    while (data.contextCalculationIndex < evalContext.index) {
      const code = workflow.at(data.contextCalculationIndex);
      const stackItem = data.stack.at(data.contextCalculationIndex);
      if (code?.t === "event" && stackItem?.t === "event") {
        code.bindings?.forEach((x) => {
          data.context[x.var] = stackItem.payload[x.index];
        });
        data.returnValue = stackItem.name;

        if (code.control === "return") {
          data.returned = true;
        }
      }

      if (code?.t === "anti-timeout" && stackItem?.t === "anti-timeout") {
        const consequenceData = stackItem.data;
        data.returnValue = consequenceData.name;

        const consequence = stackItem.consequence;
        if (consequence.control === Code.Control.fail) {
          const retryIndex = findRetryOnStack(data.contextCalculationIndex);
          if (retryIndex === null) {
            throw new Error("cannot find retry while dealing with ");
          }
          resetIndex(evalContext, retryIndex + 1);
          continue; // important
        } else if (consequence.control === "return") {
          data.returned = true;
        }
      }

      data.contextCalculationIndex += 1;
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

  // Timeout helpers

  const attemptTimeout = (evalContext: EvalContext) => {
    const timedout = Array.from(data.activeTimeout)
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
            `attempt timeout fatal error: stimeout not found at index ${index}`
          );
        }

        const antiIndex = index + ctimeout.pairOffsetIndex;
        const dueDate = stimeout.startedAt.getTime() + ctimeout.duration;
        const lateness = Date.now() - dueDate;

        return { stimeout, ctimeout, lateness, antiIndex };
      })
      .filter(({ lateness }) => lateness > 0);

    if (timedout.length > 0) {
      const lastTimedout = timedout.sort(
        (a, b) => b.antiIndex - a.antiIndex
      )[0];
      data.stack[lastTimedout.antiIndex] = {
        t: "anti-timeout",
        consequence: lastTimedout.ctimeout.consequence,
        data: {
          t: "event",
          name: lastTimedout.ctimeout.consequence.name,
          payload: {},
        },
      };
      evalContext.index = lastTimedout.antiIndex + 1;
      data.executionIndex = lastTimedout.antiIndex + 1;
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

    return false;
  };

  const evaluate = (evalContext: EvalContext) => {
    while (true) {
      attemptTimeout(evalContext);
      recalculateResult(evalContext);
      if (data.returned) break;

      const shouldContinue = evaluateImpl(evalContext);

      if (shouldContinue) continue;

      break;
    }
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

  const tickParallel = (code: CParallel, e: EEvent | null) => {
    let eIsFed = false;
    const atStack = ((): SParallel => {
      const atStack = data.stack.at(data.executionIndex);
      if (!atStack) {
        const newAtStack: SParallel = {
          t: "par",
          fulfilled: false,
          instances: [],
          nextEvalIndex: data.executionIndex + code.pairOffsetIndex + 1,
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
      const eventCode = workflow.at(data.executionIndex + code.firstEventIndex);
      if (eventCode?.t !== "event") {
        throw new Error("parallel.firstEventIndex is not event code");
      }
      const evalContext = {
        index: data.executionIndex + code.firstEventIndex,
      };
      if (feedEvent(evalContext, eventCode, e)) {
        atStack.instances.push({ executionIndex: evalContext.index });
        eIsFed = true;
      }

      // instances resumption
      if (!eIsFed) {
        const firstMatching = atStack.instances
          .map(
            (instance) =>
              [instance, workflow.at(instance.executionIndex)] as const
          )
          .filter(
            (pair): pair is [SParallelExecution, CEvent] =>
              pair[1]?.t === "event" && pair[1]?.name === e.name
          )
          .at(0);

        if (firstMatching) {
          const [instance, code] = firstMatching;
          const evalContext = {
            index: data.executionIndex + instance.executionIndex,
          };
          feedEvent(evalContext, code, e);
          instance.executionIndex = evalContext.index;
          eIsFed = true;
        }
      }
    }

    // fulfilled calculation
    const execDoneCount = atStack.instances.filter(
      (x) => x.executionIndex >= code.pairOffsetIndex
    ).length;
    const minCriteria = Math.max(code.count?.min || 1, 1);
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

      evaluate(evalContext);

      // if some next event is fed
      atStack.nextEvalIndex = evalContext.index;
      if (eventFed) {
        data.executionIndex = evalContext.index;
        return;
      }
    }
  };

  const tick = (e: EEvent | null) => {
    const evalContext = { index: data.executionIndex };
    const code = workflow.at(evalContext.index);
    if (code?.t === "par") {
      return tickParallel(code, e);
    }

    if (code?.t === "event" && e) {
      feedEvent(evalContext, code, e);
    }
    evaluate(evalContext);
    data.executionIndex = evalContext.index;
  };

  const state = () => ({
    state: data.returnValue,
    context: data.context,
  });

  const returned = () => data.returned;

  return { tick, state, returned, evaluate };
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
      state: Ev.request,
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
        state: Ev.reqStorage,
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });

      await sleep(TIMEOUT_DURATION + 100);
      machine.tick(null);
      expect(machine.returned()).toBe(false);
      expect(machine.state()).toEqual({
        state: Ev.request,
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
        state: Ev.reqStorage,
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });

      await sleep(TIMEOUT_DURATION + 100);
      machine.tick(null);
      expect(machine.returned()).toBe(true);
      expect(machine.state()).toEqual({
        state: Ev.cancelled,
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
        state: Ev.reqStorage,
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });

      machine.tick(Emit.event(Ev.bid, {}));

      await sleep(TIMEOUT_DURATION + 100);
      machine.tick(null);
      expect(machine.state()).toEqual({
        state: Ev.bid,
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });
    });
  });
});
