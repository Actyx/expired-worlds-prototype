import { describe, expect, it } from "@jest/globals";
import { v4 as uuidv4 } from "uuid";

const Enum = <
  T extends readonly string[],
  Res = Readonly<{ [S in T[number] as S]: Readonly<S> }>
>(
  strs: T
): Res =>
  strs.reduce((acc, x) => {
    acc[x] = x;
    return acc;
  }, {} as any);
type Enum<T extends object> = (typeof Ev)[keyof typeof Ev];

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

type CItem = CAnti | CEvent | CRetry | CTimeout; // | CAnti;
type CAnti = CAntiRetry | CAntiTimeout; //;

type CEventBinding = { var: string; index: string };
type CEvent = {
  t: "event";
  name: Ev;
  bindings?: CEventBinding[];
  control?: "fail" | "return";
};
type CRetry = { t: "retry" };
type CTimeout = { t: "timeout"; duration: number; consequence: CEvent };

type CAntiRetry = { t: "anti"; c: { t: "retry" } };
type CAntiTimeout = { t: "anti"; c: { t: "timeout" } };

const Code: CItem[] = [
  {
    t: "event",
    name: Ev.request,
    bindings: [
      { var: "src", index: "from" },
      { var: "dst", index: "to" },
    ],
  },
  { t: "retry" },
  // {
  //   t: "timeout",
  //   duration: 5 * 60000,
  //   consequence: {
  //     t: "event",
  //     name: "cancelled",
  //   },
  // },
  {
    t: "event",
    name: Ev.bid,
  },
  // { t: "anti", c: { t: "timeout" } },
  { t: "anti", c: { t: "retry" } },
];

// Stack
type StackItem = SEvent | SRetry | STimeout | SAntiTimeout;
type SEvent = CEvent & { payload: PEvent["payload"] };
type SRetry = CRetry;
type STimeout = CTimeout & { startedAt: Date; pairIndex: number };
type SAntiTimeout = CAntiTimeout & {
  consequence: STimeout["consequence"];
  data: PEvent;
};

// Payload

type PEvent = { t: "event"; name: Ev; payload: Record<string, unknown> };

const WFMachine = <FirstCEvent extends CEvent>(
  workflow: [FirstCEvent, ...CItem[]]
) => {
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
      if (data.returned) break;

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

  const findMatchingRetryIndex = (indexInput: number) => {
    let counter = 1;
    let index = indexInput;

    while (index >= 0) {
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
    while (index >= 0) {
      index -= 1;
      const stackItem = data.stack.at(indexInput);
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

  const evaluate = () => {
    while (true) {
      attemptTimeout();
      recalculate();
      if (data.returned) break;

      const code = workflow.at(data.executionIndex);
      if (!code) {
        data.returned = true;
        break;
      }

      // Handle Retry Code

      if (code.t === "retry") {
        data.stack[data.executionIndex] = { t: "retry" };
        data.executionIndex += 1;
        continue;
      }

      if (code.t === "anti" && code.c.t == "retry") {
        const matchingRetryIndex = findMatchingRetryIndex(data.executionIndex);
        if (typeof matchingRetryIndex !== "number") {
          throw new Error("retry not found");
        }
        data.stack[matchingRetryIndex] = null;
        data.executionIndex += 1;
        continue;
      }

      if (code.t === "timeout") {
        const pair = findMatchingAntiTimeout(data.executionIndex);
        if (typeof pair !== "number") {
          throw new Error("anti-timeout not found");
        }
        data.stack[data.executionIndex] = {
          t: "timeout",
          pairIndex: pair,
          consequence: code.consequence,
          duration: code.duration,
          startedAt: new Date(),
        };
        data.activeTimeout.add(data.executionIndex);
        data.executionIndex += 1;
        continue;
      }

      break;
    }
  };

  const feed = (e: PEvent) => {
    const code = workflow.at(data.executionIndex);
    if (code?.t === "event" && e.name === code.name) {
      data.stack[data.executionIndex] = {
        t: "event",
        name: e.name,
        payload: e.payload,
        bindings: code.bindings,
      };
      data.executionIndex += 1;
      evaluate();
    }
  };

  const state = () => {
    const ev = (data.stack
      .slice(0)
      .reverse()
      .find((item) => item && item.t === "event") || null) as SEvent | null;

    return {
      state: ev?.name || null,
      context: data.context,
    };
  };

  const returned = () => data.returned;

  return { feed, state, returned, evaluate };
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
      {
        t: "event",
        name: Ev.request,
        bindings: [
          { var: "src", index: "from" },
          { var: "dst", index: "to" },
        ],
      },
    ]);

    machine.feed({
      t: "event",
      name: Ev.request,
      payload: { from: "storage-1", to: "storage-2" },
    });

    expect(machine.returned()).toBe(true);
    expect(machine.state()).toEqual({
      state: Ev.request,
      context: { src: "storage-1", dst: "storage-2" },
    });
  });
});
