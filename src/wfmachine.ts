import { ActyxWFBusiness, CTypeProto, sortByEventKey } from "./consts.js";
import { MultiverseTree } from "./reality.js";
import {
  Actor,
  CAntiParallel,
  CAntiRetry,
  CAntiTimeout,
  CChoice,
  CCompensationFastQuery,
  CEvent,
  CItem,
  CMatch,
  CParallel,
  CRetry,
  CTimeout,
  Code,
  Exact,
  Otherwise,
  WFWorkflow,
  validateBindings,
} from "./wfcode.js";

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
  id: string;
  payload: EEvent<CType>["payload"];
};
type SRetry = Pick<CRetry, "t">;
type SParallelExecution<CType extends CTypeProto> = {
  entry: EEvent<CType>[];
  // resultCalcIndex: number;
  // executionIndex: number;
  // firstProcessedEvent: ActyxWFBusiness<CType>;
  // lastProcessedEvent: ActyxWFBusiness<CType>;
};

type SParallel<CType extends CTypeProto> = Pick<CParallel, "t"> & {
  lastEventId: string;
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

export type EEvent<CType extends CTypeProto> = {
  t: "event";
  id: string;
  name: CType["ev"];
  payload: Record<string, unknown>;
};
export namespace Emit {
  export const event = <CType extends CTypeProto>(
    id: string,
    name: CType["ev"],
    payload: Record<string, unknown>
  ): EEvent<CType> => ({
    id,
    t: "event",
    name,
    payload,
  });

  export const fromWFEvent = <CType extends CTypeProto>(
    ev: ActyxWFBusiness<CType>
  ) => Emit.event(ev.meta.eventId, ev.payload.t, ev.payload.payload);
}

export const One: unique symbol = Symbol("One");
export const Parallel: unique symbol = Symbol("Parallel");
export type One<T = unknown> = [typeof One, T];
export type Parallel<T = unknown> = [typeof Parallel, T[]];

type StateName<T> = One<T> | Parallel<T>;
export type State<T> = {
  state: StateName<T> | null;
  context: Record<string, unknown>;
};
export const statesAreEqual = <T extends string>(a: State<T>, b: State<T>) => {
  if (a.state !== b.state) return false;
  const allKeys = Array.from(new Set(Object.keys(a).concat(Object.keys(b))));
  const foundDiscrepancyIndex = allKeys.findIndex(
    (key) => a.context[key] !== b.context[key]
  );
  return foundDiscrepancyIndex === -1; // -1 means not found in the context of `findIndex`
};

export type WFMachine<CType extends CTypeProto> = {
  tick: (state: EEvent<CType>) => boolean;
  state: () => State<CType["ev"]>;
  returned: () => boolean;
  availableTimeout: () => {
    ctimeout: CTimeout<CType>;
    lateness: number;
  }[];
  activeCompensationCode: () => {
    codeIndex: number;
    firstEventId: string;
  }[];
  availableCompensateable: () => {
    actor: Actor<CType>;
    name: CType["ev"];
  }[];
  availableCommands: () => {
    actor: Actor<CType>;
    name: CType["ev"];
    control?: Code.Control;
    reason: null | "compensation" | "timeout";
  }[];
};

export const WFMachine = <CType extends CTypeProto>(
  self: { id: string },
  wfWorkflow: WFWorkflow<CType>,
  multiverse: MultiverseTree.Type<CType>
): WFMachine<CType> => {
  validateBindings(wfWorkflow);
  const workflow = wfWorkflow.code;
  const compensateFastQuery = CCompensationFastQuery.make(workflow);

  const data = {
    resultCalcIndex: 0,
    executionIndex: 0,
    lastProcessedEvent: null as null | ActyxWFBusiness<CType>,
    stack: [] as (StackItem<CType> | null)[],
  };

  const active = {
    activeTimeout: new Set() as Set<number>,
    activeCompensation: new Set() as Set<number>,
  };

  const innerstate = {
    context: {} as Record<string, unknown>,
    returnValue: null as StateName<CType["ev"]> | null,
    returned: false,
  };

  const resetIndex = (evalContext: EvalContext, targetIndex: number) => {
    // set execution back at the index
    data.stack.length = targetIndex;
    data.executionIndex = targetIndex;
    evalContext.index = targetIndex;

    // remove timeouts after the last item index in stack
    active.activeTimeout = new Set(
      Array.from(active.activeTimeout).filter(
        (index) => index >= data.stack.length
      )
    );

    // force recalculate context
    data.resultCalcIndex = 0;
    innerstate.context = {};
  };

  // Finders helpers

  const nullifyMatchingTimeout = (
    antiTimeout: CAntiTimeout,
    indexInput: number
  ) => {
    const timeoutIndex = antiTimeout.pairOffsetIndex + indexInput;
    const maybeTimeout = data.stack.at(timeoutIndex);

    if (
      active.activeTimeout.has(timeoutIndex) &&
      maybeTimeout?.t === "timeout"
    ) {
      active.activeTimeout.delete(timeoutIndex);
      active.activeTimeout.delete(timeoutIndex);
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

  const getLastEventIdForParallel = (index: number) => {
    let i = index;
    /**
     * counter if whether the index is inside a parallel relative to initial
     * index block level
     */
    let inParallel = 0;
    while (i >= 0) {
      const code = workflow.at(i);
      const atStack = data.stack.at(i);
      if (code?.t === "anti-par") {
        inParallel += 1;
      }

      if (code?.t === "par") {
        if (inParallel > 0) {
          inParallel -= 1;
        } else {
          if (atStack?.t === "par") {
            return atStack.lastEventId;
          }
        }
      }

      if (code?.t === "event" && atStack?.t === "event") {
        if (inParallel === 0) {
          return atStack.id;
        }
      }
      i--;
    }
    return null;
  };

  /**
   * Find all active timeouts
   */
  const availableTimeouts = () =>
    Array.from(active.activeTimeout)
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

        return { timeoutIndex: index, stimeout, ctimeout, lateness, antiIndex };
      })
      .sort((a, b) => {
        // in case of nested timeouts: sorted by last / outermost timeout
        return b.antiIndex - a.antiIndex;
      });

  /**
   * Find all active timeouts that is late. This is useful for telling which timeout commands are available to the users.
   */

  const activeCompensationCode = () =>
    Array.from(active.activeCompensation)
      .map((codeIndex) => {
        const ccompensate = workflow.at(codeIndex);
        if (ccompensate?.t !== "compensate") {
          throw new Error(
            `compensate query fatal error: ctimeout not found at index ${codeIndex}`
          );
        }

        const withIndex = codeIndex + ccompensate.withIndexOffset;
        const firstEventCodeIndex = (() => {
          let i = codeIndex;
          while (i < withIndex) {
            const code = workflow.at(i);
            if (code && code.t === "event") {
              return i;
            }
            i++;
          }
          throw new Error(`compensate query fatal error: cevent not found`);
        })();
        const firstEventAtStack = data.stack.at(firstEventCodeIndex);
        if (firstEventAtStack?.t !== "event") {
          throw new Error("compensate first event at stack type mismatch");
        }

        return { codeIndex, firstEventId: firstEventAtStack.id };
      })
      .sort((a, b) => {
        // in case of nested timeouts: sorted by last / innermost compensation
        return b.codeIndex - a.codeIndex;
      });

  /**
   * Find all active compensateable
   * TODO: return next-events of compensate-with as compensations
   */
  const availableCompensateable = () =>
    Array.from(active.activeCompensation)
      .map((index) => {
        const ccompensate = workflow.at(index);
        if (ccompensate?.t !== "compensate") {
          throw new Error(
            `compensate query fatal error: ctimeout not found at index ${index}`
          );
        }

        const compensationIndex = index;
        const withIndex = ccompensate.withIndexOffset;
        const antiIndex = index + ccompensate.antiIndexOffset;
        const firstCompensationIndex = index + ccompensate.withIndexOffset + 1;
        const firstCompensation = workflow.at(firstCompensationIndex);
        if (firstCompensation?.t !== "event") {
          throw new Error(
            `compensate query fatal error: compensation's first code is not of type event`
          );
        }

        return {
          compensationIndex,
          code: ccompensate,
          firstCompensation,
          firstCompensationIndex,
          antiTimeoutIndex: antiIndex,
          withIndex,
        };
      })
      .filter((x) => {
        // only compensateable that's not
        return data.executionIndex < x.withIndex;
      })
      .sort((a, b) => {
        // in case of nested compensations: sort by last/innermost timeout
        return b.compensationIndex - a.compensationIndex;
      });

  /**
   * extract available commands, recursesively when needed
   */
  const extractAvailableCommandFromSingularCode = (
    index: number,
    result: ReturnType<WFMachine<CType>["availableCommands"]>,
    code: CItem<CType>,
    overrideReason?: null | "compensation" | "timeout"
  ) => {
    if (code?.t === "par") {
      const { atStack, maxReached, minReached } = fetchParallelCriteria(
        { index: data.executionIndex },
        code
      );

      if (!maxReached) {
        const index = data.executionIndex + code.firstEventIndex;
        const eventCode = workflow.at(index);
        if (eventCode) {
          extractAvailableCommandFromSingularCode(index, result, eventCode);
        }
      }

      atStack.instances.map((instance) => {
        const index =
          data.executionIndex + code.firstEventIndex + instance.entry.length;
        const eventCode = workflow.at(index);
        if (eventCode) {
          extractAvailableCommandFromSingularCode(index, result, eventCode);
        }
      });

      if (minReached) {
        const maybeCEv = workflow.at(atStack.nextEvalIndex);
        if (maybeCEv) {
          extractAvailableCommandFromSingularCode(
            atStack.nextEvalIndex,
            result,
            maybeCEv
          );
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
      result.push({
        name,
        actor: code.actor,
        control,
        reason:
          overrideReason ||
          (compensateFastQuery.isInsideWithBlock(index)
            ? "compensation"
            : null),
      });
    }

    if (code?.t === "choice") {
      const eventStartIndex = data.executionIndex + 1;
      const antiIndex = data.executionIndex + code.antiIndexOffset;
      workflow
        .slice(eventStartIndex, antiIndex) // take the CEvent between the choice and anti-choice
        .forEach((x, index) => {
          if (x.t !== "event") {
            // defensive measure, should not exist
            throw new Event("codes inside are not CEvent");
          }

          const codeIndex = eventStartIndex + index;

          extractAvailableCommandFromSingularCode(codeIndex, result, x);
        });
    }
  };

  const availableCommands = (): ReturnType<
    WFMachine<CType>["availableCommands"]
  > => {
    const code = workflow.at(data.executionIndex);
    const result: ReturnType<WFMachine<CType>["availableCommands"]> = [];

    if (code) {
      extractAvailableCommandFromSingularCode(
        data.executionIndex,
        result,
        code
      );
    }

    availableTimeouts().forEach((timeout) => {
      extractAvailableCommandFromSingularCode(
        timeout.timeoutIndex,
        result,
        timeout.ctimeout.consequence,
        "timeout"
      );
    });

    // TODO: review this code
    availableCompensateable().forEach((compensation) => {
      extractAvailableCommandFromSingularCode(
        compensation.compensationIndex,
        result,
        compensation.firstCompensation,
        "compensation"
      );
    });

    return result;
  };

  const fetchParallelCriteria = (
    evalContext: EvalContext,
    parallelCode: CParallel
  ) => {
    const {
      count: { max, min },
      pairOffsetIndex,
    } = parallelCode;

    const evLength = pairOffsetIndex - 1;

    const minCriteria = Math.max(min !== undefined ? min : 0, 0);
    const maxCriteria = Math.min(max !== undefined ? max : Infinity, Infinity);

    const atStack = ((): SParallel<CType> => {
      const atStack = data.stack.at(evalContext.index);
      if (!atStack) {
        const lastEventId = getLastEventIdForParallel(evalContext.index - 1);
        if (lastEventId === null) {
          // TODO: maybe support parallel at the beginning?
          // but I don't think that makes sense
          throw new Error(
            "impossible right now. parallel should have been preceeded with a single event. this should have been prevented at the CCode building and validating"
          );
        }
        const newAtStack: SParallel<CType> = {
          t: "par",
          instances: [],
          lastEventId,
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
      (instance) => instance.entry.length >= evLength
    ).length;

    const maxReached = execDoneCount >= maxCriteria;
    const minReached = execDoneCount >= minCriteria;

    return { atStack, maxReached, minReached, evLength, max, min };
  };

  const recalculatePar = (stackItem: SParallel<CType>) => {
    innerstate.returnValue = [
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
          const index = x.index;
          innerstate.context[x.var] = stackItem.payload[index];
        });
        innerstate.returnValue = [One, code.name];

        if (code.control === "return") {
          innerstate.returned = true;
        }
      }

      if (code?.t === "anti-timeout" && stackItem?.t === "anti-timeout") {
        const consequenceData = stackItem.data;
        innerstate.returnValue = [One, consequenceData.name];

        const consequence = stackItem.consequence;
        if (consequence.control === Code.Control.fail) {
          const retryIndex = findRetryOnStack(data.resultCalcIndex);
          if (retryIndex === null) {
            throw new Error("cannot find retry while dealing with ");
          }
          resetIndex(evalContext, retryIndex + 1);
          continue; // important
        } else if (consequence.control === "return") {
          innerstate.returned = true;
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
      innerstate.returned = true;
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
      active.activeTimeout.add(evalContext.index);
      evalContext.index += 1;
      return true;
    }

    if (code.t === "anti-timeout") {
      nullifyMatchingTimeout(code, evalContext.index);
      evalContext.index += 1;
      return true;
    }

    if (code.t === "compensate") {
      active.activeCompensation.add(evalContext.index);
      evalContext.index += 1;
      return true;
    }

    if (code.t === "compensate-end") {
      const compensateIndex = evalContext.index + code.baseIndexOffset;
      const rightAfterAntiIndex = evalContext.index + code.antiIndexOffset + 1;
      active.activeCompensation.delete(compensateIndex);
      evalContext.index = rightAfterAntiIndex;
      return true;
    }

    if (code.t === "anti-compensate") {
      const compensateIndex = evalContext.index + code.baseIndexOffset;
      active.activeCompensation.delete(compensateIndex);
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
        inner: WFMachine<CType>(self, code.subworkflow, multiverse),
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
            (x.matchCase.case[0] === Exact &&
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
      if (innerstate.returned) break;

      const shouldContinue = evaluateImpl(evalContext);

      if (shouldContinue) continue;

      break;
    }
  };

  const feedCompensation = (evalContext: EvalContext, e: EEvent<CType>) => {
    const firstMatching = availableCompensateable()
      .filter((x) => x.firstCompensation.name === e.name)
      .at(0);

    if (firstMatching) {
      const firstMatchingIndex = firstMatching.firstCompensationIndex;
      active.activeCompensation.delete(firstMatching.compensationIndex);
      evalContext.index = firstMatchingIndex;
      data.executionIndex = firstMatchingIndex;
      return feedEvent(evalContext, firstMatching.firstCompensation, e);
    }
    return false;
  };

  const feedTimeout = (evalContext: EvalContext, e: EEvent<CType>) => {
    const lastMatching = availableTimeouts()
      .filter((x) => x.ctimeout.consequence.name === e.name)
      .at(0);

    if (lastMatching) {
      data.stack[lastMatching.antiIndex] = {
        t: "anti-timeout",
        consequence: lastMatching.ctimeout.consequence,
        data: {
          id: e.id,
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
        id: e.id,
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
        id: e.id,
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

  const tickInnerParallelProcessInstances = (
    evalContext: EvalContext,
    parallelCode: CParallel,
    e: EEvent<CType>
  ) => {
    let fed = false;
    const { atStack, evLength } = fetchParallelCriteria(
      evalContext,
      parallelCode
    );

    // New instances detection
    const firstEventCode = workflow.at(
      evalContext.index + parallelCode.firstEventIndex
    );
    if (firstEventCode?.t !== "event") {
      throw new Error("parallel.firstEventIndex is not event code");
    }
    const registeredFirstEventIds = new Set(
      atStack.instances.map((x) => x.entry[0].id)
    );
    const newFirstEvents = multiverse
      .getNextById(atStack.lastEventId)
      .filter(
        (ev) =>
          e.name === firstEventCode.name &&
          !registeredFirstEventIds.has(ev.meta.eventId)
      );

    newFirstEvents.forEach((e) => {
      const newInstance: SParallelExecution<CType> = {
        entry: [Emit.fromWFEvent(e)],
      };
      atStack.instances.push(newInstance);
      fed = true;
    });

    // Advance registered parallel instances
    atStack.instances.forEach((x) => {
      while (true) {
        const index = x.entry.length;
        if (index >= evLength) return;
        const code = workflow.at(index);
        if (code?.t !== "event") return;
        const last = x.entry[x.entry.length - 1].id;
        const validNext = multiverse
          .getNextById(last)
          .filter((ev) => ev.payload.t === code.name);
        const sortedNext = sortByEventKey(validNext).at(0);
        if (!sortedNext) return;
        x.entry.push(Emit.fromWFEvent(sortedNext));
        fed = true;
      }
    });

    return { fed };
  };

  const tickInnerParallelRecursive = (
    evalContext: EvalContext,
    parallelCode: CParallel,
    e: EEvent<CType>
  ): { nextEvaluated: null | number; fed: boolean } => {
    let nextEvaluated: number | null = null;

    let { fed } = tickInnerParallelProcessInstances(
      evalContext,
      parallelCode,
      e
    );

    const { atStack, min, minReached, maxReached, evLength } =
      fetchParallelCriteria(evalContext, parallelCode);

    // Check if the parallel should have advanced.
    // Predecessor depends if `min` is 0 or not.
    // If `min` is 0, the event before the parallel can be the predecessor of the event after the parallel
    const predecessorIds = atStack.instances
      .filter((instance) => instance.entry.length === evLength)
      .map((instance) => instance.entry[instance.entry.length - 1])
      .map((ev) => ev.id);

    if (min === 0) {
      predecessorIds.push(atStack.lastEventId);
    }

    // advance signal from predecessors

    if (minReached) {
      // Can advance, how to advance? It depends if `min` is 0 or not.
      const nextEvalContext = { index: atStack.nextEvalIndex };
      // move nextEvalIndex as far as it can
      evaluate(nextEvalContext);

      const innerCode = workflow.at(nextEvalContext.index);
      if (!fed && innerCode && e) {
        if (innerCode.t === "par") {
          const res = tickInnerParallelRecursive(nextEvalContext, innerCode, e);
          fed = res.fed;
          if (fed) {
            nextEvaluated = nextEvalContext.index;
          }

          nextEvaluated = res.nextEvaluated;
        } else {
          fed = feed(nextEvalContext, innerCode, e);
          if (fed) {
            nextEvaluated = nextEvalContext.index;
          }
        }
      }

      atStack.nextEvalIndex = nextEvalContext.index;
    }

    if (maxReached) {
      // TODO: change advance code
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

  const tickParallel = (parallelCode: CParallel, e: EEvent<CType>): boolean => {
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

  const tickMatch = (_: CMatch<CType>, e: EEvent<CType>): boolean => {
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

  const tick = (e: EEvent<CType>): boolean => {
    const evalContext = { index: data.executionIndex };
    const code = workflow.at(evalContext.index);
    let fed = false;
    if (code?.t === "match") {
      return tickMatch(code, e);
    }

    if (code?.t === "par") {
      return tickParallel(code, e);
    }

    if (code) {
      fed = feed(evalContext, code, e);
    }

    evaluate(evalContext);
    data.executionIndex = evalContext.index;
    return fed;
  };

  const state = () => {
    const evalContext = { index: data.executionIndex };
    const atStack = getSMatchAtIndex(evalContext.index);
    if (atStack) return atStack.inner.state();

    return {
      state: innerstate.returnValue,
      context: innerstate.context,
    };
  };

  const returned = () => {
    const evalContext = { index: data.executionIndex };
    const atStack = getSMatchAtIndex(evalContext.index);
    if (atStack) return atStack.inner.returned();

    return innerstate.returned;
  };

  const reset = () => {
    data.executionIndex = 0;
    data.resultCalcIndex = 0;
    data.stack = [];
    data.lastProcessedEvent = null;

    active.activeCompensation = new Set();
    active.activeTimeout = new Set();

    innerstate.context = {};
    innerstate.returnValue = null;
    innerstate.returned = false;
  };

  const advance = () => {
    const next = () => {
      const last = data.lastProcessedEvent;
      if (!last) return multiverse.getCanonChain();
      return multiverse.nextMostCanonChain(last);
    };
    if (!next) throw new Error("cannot advance machine, chain not found");
  };

  return {
    tick,
    state,
    returned,
    availableTimeout: availableTimeouts,
    availableCommands: availableCommands,
    activeCompensationCode: activeCompensationCode,
    availableCompensateable: () =>
      availableCompensateable().map((x) => ({
        name: x.firstCompensation.name,
        actor: x.firstCompensation.actor,
      })),
  };
};
