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
type CParallel = { t: "par" };
type CRetry = { t: "retry" };
type CTimeout = { t: "timeout"; duration: number; consequence: CEvent };

type CAntiRetry = { t: "anti"; c: { t: "retry" } };
type CAntiTimeout = { t: "anti"; c: { t: "timeout" } };
type CAntiParallel = { t: "anti"; c: { t: "par" } };

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
    { t: "retry" },
    ...workflow,
    { t: "anti", c: { t: "retry" } },
  ];

  export const timeout = (
    duration: number,
    workflow: CItem[],
    consequence: CEvent
  ): CItem[] => [
    { t: "timeout", duration, consequence },
    ...workflow,
    { t: "anti", c: { t: "timeout" } },
  ];
}

// Stack
type StackItem = SEvent | SRetry | STimeout | SAntiTimeout;
type SEvent = CEvent & { payload: EEvent["payload"] };
type SRetry = CRetry;
type STimeout = CTimeout & { startedAt: Date; pairIndex: number };
type SAntiTimeout = CAntiTimeout & {
  consequence: STimeout["consequence"];
  data: EEvent;
};

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

  const resetIndex = (targetIndex: number) => {
    // set execution back at the index
    data.stack.length = targetIndex;
    data.executionIndex = targetIndex;

    // remove timeouts after the last item index in stack
    data.activeTimeout = new Set(
      Array.from(data.activeTimeout).filter(
        (index) => index >= data.stack.length
      )
    );

    // recalculate context
    data.contextCalculationIndex = 0;
    data.context = {};
  };

  const recalculate = () => {
    while (data.contextCalculationIndex < data.executionIndex) {
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

      if (
        code?.t === "anti" &&
        code.c.t === "timeout" &&
        stackItem?.t === "anti" &&
        stackItem.c.t === "timeout"
      ) {
        const consequenceData = stackItem.data;
        data.returnValue = consequenceData.name;

        const consequence = stackItem.consequence;
        if (consequence.control === "fail") {
          const retryIndex = findRetryOnStack(data.contextCalculationIndex);
          if (retryIndex === null) {
            throw new Error("cannot find retry while dealing with ");
          }
          resetIndex(retryIndex + 1);
          continue; // important
        } else if (consequence.control === "return") {
          data.returned = true;
        }
      }

      data.contextCalculationIndex += 1;
    }
  };

  const findMatchingAntiTimeout = (indexInput: number) => {
    let counter = 1;
    let index = indexInput;
    while (index < workflow.length) {
      index += 1;
      const code = workflow.at(index);
      if (code?.t === "timeout") {
        counter += 1;
      }
      if (code?.t === "anti" && code.c.t === "timeout") {
        counter -= 1;
        if (counter === 0) {
          return index;
        }
      }
    }

    return null;
  };

  const nullifyMatchingTimeout = (indexInput: number) => {
    const foundPair = Array.from(data.activeTimeout)
      .map(
        (timeoutIndex) => [timeoutIndex, data.stack.at(timeoutIndex)] as const
      )
      .find(
        (x): x is [number, STimeout] =>
          x[1]?.t === "timeout" && x[1].pairIndex === indexInput
      );

    if (foundPair) {
      const [timeoutIndex, _] = foundPair;
      data.activeTimeout.delete(timeoutIndex);
      data.stack[timeoutIndex] = null;
    } else {
      throw new Error("timeout not found on stack");
    }
  };

  const findMatchingRetryIndex = (indexInput: number) => {
    let counter = 1;
    let index = indexInput;

    while (index > 0) {
      index -= 1;
      const code = workflow.at(index);
      if (code?.t === "anti" && code.c.t === "retry") {
        counter += 1;
      }
      if (code?.t === "retry") {
        counter -= 1;
        if (counter === 0) {
          return index;
        }
      }
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

  const attemptTimeout = () => {
    const timedout = Array.from(data.activeTimeout)
      .map((index) => {
        const timeout = data.stack.at(index);
        if (timeout?.t !== "timeout") {
          throw new Error(
            `attempt timeout fatal error: timeout not found at index ${index}`
          );
        }

        const dueDate = timeout.startedAt.getTime() + timeout.duration;
        const lateness = Date.now() - dueDate;

        return { item: timeout, lateness };
      })
      .filter(({ lateness }) => lateness > 0)
      .map((x) => x.item);

    if (timedout.length > 0) {
      const lastTimedout = timedout.sort(
        (a, b) => b.pairIndex - a.pairIndex
      )[0];
      data.stack[lastTimedout.pairIndex] = {
        t: "anti",
        c: { t: "timeout" },
        consequence: lastTimedout.consequence,
        data: {
          t: "event",
          name: lastTimedout.consequence.name,
          payload: {},
        },
      };
      data.executionIndex = lastTimedout.pairIndex + 1;
    }
  };

  type Continue = boolean;
  const evaluateImpl = (indexer: { index: number }): Continue => {
    // Handle Retry Code
    const code = workflow.at(indexer.index);
    if (!code) {
      data.returned = true;
      return false;
    }

    if (code.t === "retry") {
      data.stack[indexer.index] = { t: "retry" };
      indexer.index += 1;
      return true;
    }

    if (code.t === "anti" && code.c.t == "retry") {
      const matchingRetryIndex = findMatchingRetryIndex(indexer.index);
      if (typeof matchingRetryIndex !== "number") {
        throw new Error("retry not found");
      }
      data.stack[matchingRetryIndex] = null;
      indexer.index += 1;
      return true;
    }

    // Handle timeout code

    if (code.t === "timeout") {
      const pair = findMatchingAntiTimeout(indexer.index);
      if (typeof pair !== "number") {
        throw new Error("anti-timeout not found");
      }
      data.stack[indexer.index] = {
        t: "timeout",
        pairIndex: pair,
        consequence: code.consequence,
        duration: code.duration,
        startedAt: new Date(),
      };
      data.activeTimeout.add(indexer.index);
      indexer.index += 1;
      return true;
    }

    if (code.t === "anti" && code.c.t === "timeout") {
      nullifyMatchingTimeout(indexer.index);
      indexer.index += 1;
      return true;
    }

    return false;
  };

  const evaluate = () => {
    while (true) {
      attemptTimeout();
      recalculate();
      if (data.returned) break;

      const mutableIndexer = { index: data.executionIndex };
      const shouldContinue = evaluateImpl(mutableIndexer);
      data.executionIndex = mutableIndexer.index;

      if (shouldContinue) continue;

      break;
    }
  };

  const tick = (e: EEvent | null) => {
    if (e) {
      const code = workflow.at(data.executionIndex);
      if (code?.t === "event" && e.name === code.name) {
        data.stack[data.executionIndex] = {
          t: "event",
          name: e.name,
          payload: e.payload,
          bindings: code.bindings,
        };
        data.executionIndex += 1;
      }
    }
    evaluate();
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
      const TIMEOUT_DURATION = 3000;

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
      const TIMEOUT_DURATION = 3000;

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
      const TIMEOUT_DURATION = 3000;

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
