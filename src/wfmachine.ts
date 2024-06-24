import { ActyxWFBusiness, CTypeProto, sortByEventKey } from "./consts.js";
import { createLinearChain } from "./event-utils.js";
import { MultiverseTree } from "./reality.js";
import {
  Actor,
  CAntiParallel,
  CAntiRetry,
  CAntiTimeout,
  CChoice,
  CCompensationIndexer,
  CEvent,
  CItem,
  CMatch,
  CParallel,
  CParallelIndexer,
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
  event: ActyxWFBusiness<CType>;
};

type SRetry = Pick<CRetry, "t">;
type SParallel<CType extends CTypeProto> = Pick<CParallel, "t"> & {
  lastEvent: ActyxWFBusiness<CType>;
  nextEvalIndex: number;
  instances: SParallelExecution<CType>[];
};
type SParallelExecution<CType extends CTypeProto> = {
  entry: ActyxWFBusiness<CType>[];
};

type STimeout<CType extends CTypeProto> = Pick<CTimeout<CType>, "t"> & {
  startedAt: Date;
};
type SAntiTimeout<CType extends CTypeProto> = Pick<CAntiTimeout, "t"> & {
  consequence: CTimeout<CType>["consequence"];
  event: ActyxWFBusiness<CType>;
};
type SAntiParallel = Pick<CAntiParallel, "t"> & {};

// Payload

export const One: unique symbol = Symbol("One");
export const Parallel: unique symbol = Symbol("Parallel");
export type One<T = unknown> = [typeof One, T];
export type Parallel<T = unknown> = [typeof Parallel, T, T[]];

type State<CType extends CTypeProto> =
  | One<ActyxWFBusiness<CType>>
  | Parallel<ActyxWFBusiness<CType>>;

export type WFMachineState<CType extends CTypeProto> = {
  state: State<CType> | null;
  context: Record<string, unknown>;
};
export const statesAreEqual = <CType extends CTypeProto>(
  a: WFMachineState<CType>,
  b: WFMachineState<CType>
) => {
  if (a.state !== b.state) return false;
  const allKeys = Array.from(new Set(Object.keys(a).concat(Object.keys(b))));
  const foundDiscrepancyIndex = allKeys.findIndex(
    (key) => a.context[key] !== b.context[key]
  );
  return foundDiscrepancyIndex === -1; // -1 means not found in the context of `findIndex`
};

export type WFMachine<CType extends CTypeProto> = {
  tick: (input: ActyxWFBusiness<CType> | null) => boolean;
  state: () => WFMachineState<CType>;
  latestStateEvent: () => ActyxWFBusiness<CType> | null;
  returned: () => boolean;
  availableTimeouts: () => {
    ctimeout: CTimeout<CType>;
    lateness: number;
  }[];
  availableCompensateable: () => {
    codeIndex: number;
    actor: Actor<CType>;
    name: CType["ev"];
    fromTimelineOf: string;
  }[];
  availableNexts: () => {
    actor: Actor<CType>;
    name: CType["ev"];
    control?: Code.Control;
    reason: null | "compensation" | "timeout" | "parallel";
  }[];
  advanceToMostCanon: () => void;
  resetAndAdvanceToMostCanon: () => void;
  resetAndAdvanceToEventId: (eventId: string) => void;
};

export const WFMachine = <CType extends CTypeProto>(
  wfWorkflow: WFWorkflow<CType>,
  multiverse: MultiverseTree.Type<CType>
): WFMachine<CType> => {
  type TickInput = ActyxWFBusiness<CType>;
  type TickRes = { jumpToIndex: number | null; fed: boolean };

  validateBindings(wfWorkflow);

  const workflow = wfWorkflow.code;
  const ccompensateIndexer = CCompensationIndexer.make(workflow);
  const cparallelIndexer = CParallelIndexer.make(workflow);

  const data = {
    resultCalcIndex: 0,
    executionIndex: 0,
    stack: [] as (StackItem<CType> | null)[],
  };

  const active = {
    activeTimeout: new Set() as Set<number>,
    activeCompensation: new Set() as Set<number>,
  };

  const innerstate = {
    context: {} as Record<string, unknown>,
    state: null as State<CType> | null,
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
    if (code?.t === "anti-timeout") return pairIndex;
    return null;
  };

  const findMatchingRetryIndex = (retry: CAntiRetry, indexInput: number) => {
    const pairIndex = retry.pairOffsetIndex + indexInput;
    const code = workflow.at(pairIndex);
    if (code?.t === "retry") return pairIndex;
    return null;
  };

  const findRetryOnStack = (indexInput: number) => {
    let index = indexInput;
    while (index >= 0) {
      index -= 1;
      const stackItem = data.stack.at(index);
      if (stackItem?.t === "retry") return index;
    }
    return null;
  };

  const getSMatchAtIndex = (index: number) => {
    const atStack = data.stack.at(index);
    if (atStack?.t !== "match") return null;
    return atStack;
  };

  // Helpers

  const getLastEventForParallel = (
    index: number
  ): ActyxWFBusiness<CType> | null => {
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
            return atStack.lastEvent;
          }
        }
      }

      if (code?.t === "event" && atStack?.t === "event") {
        if (inParallel === 0) {
          return atStack.event;
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
   * Find all active compensateable
   * TODO: return next-events of compensate-with as compensations
   */
  const activeCompensation = () =>
    Array.from(active.activeCompensation)
      .map((index) => {
        const ccompensate = workflow.at(index);
        if (ccompensate?.t !== "compensate") {
          throw new Error(
            `compensate query fatal error: ctimeout not found at index ${index}`
          );
        }

        const compensationIndex = index;
        const firstEventIndex = index + 1;
        const firstEvent = data.stack.at(firstEventIndex);
        const withIndex = index + ccompensate.withIndexOffset;
        const antiIndex = index + ccompensate.antiIndexOffset;
        const firstCompensationIndex = index + ccompensate.withIndexOffset + 1;
        const firstCompensation = workflow.at(firstCompensationIndex);
        if (firstEvent?.t !== "event") {
          throw new Error(
            `compensate query fatal error: compensation.body first data at stack is missing`
          );
        }
        if (firstCompensation?.t !== "event") {
          throw new Error(
            `compensate query fatal error: compensation.with first code is not of type event`
          );
        }

        return {
          codeIndex: index,
          compensationIndex,
          code: ccompensate,
          firstCompensation,
          firstCompensationIndex,
          antiTimeoutIndex: antiIndex,
          withIndex,
          fromTimelineOf: firstEvent.event.meta.eventId,
        };
      })
      .sort((a, b) => {
        // in case of nested compensations: sort by last/innermost timeout
        return b.compensationIndex - a.compensationIndex;
      });

  const availableCompensateable = () =>
    activeCompensation().filter((x) => data.executionIndex < x.withIndex);

  /**
   * Extract valid next event types
   */
  const extractValidNext = (
    index: number,
    result: ReturnType<WFMachine<CType>["availableNexts"]>,
    code: CItem<CType>
  ) => {
    if (code?.t === "par") {
      // parStarts
      (() => {
        const firstIndex = index + code.firstEventIndex;
        const firstChildCode = workflow.at(firstIndex);
        if (!firstChildCode) return;
        extractValidNext(firstIndex, result, firstChildCode);
      })();

      const parStack = fetchParallelCriteria({ index }, code).atStack;
      parStack.instances.map((instance) => {
        const childIndex = index + code.firstEventIndex + instance.entry.length;
        const childCode = workflow.at(index);
        if (!childCode) return;
        extractValidNext(childIndex, result, childCode);
      });

      // nextOfPar
      (() => {
        const maybeCEv = workflow.at(parStack.nextEvalIndex);
        if (!maybeCEv) return;
        extractValidNext(parStack.nextEvalIndex, result, maybeCEv);
      })();
      return;
    }

    if (code?.t === "match") {
      const smatch = data.stack.at(data.executionIndex);
      if (smatch && smatch.t === "match") {
        result.push(...smatch.inner.availableNexts());
      }
      return;
    }

    if (code?.t === "event") {
      const { name, control } = code;
      result.push({
        name,
        actor: code.actor,
        control,
        reason: (() => {
          if (ccompensateIndexer.isInsideWithBlock(index))
            return "compensation";
          if (cparallelIndexer.isParallelStart(index)) return "parallel";
          return null;
        })(),
      });
      return;
    }

    if (code?.t === "choice") {
      const eventStartIndex = data.executionIndex + 1;
      const antiIndex = data.executionIndex + code.antiIndexOffset;
      workflow
        .map((code, line) => ({ code, line }))
        .slice(eventStartIndex, antiIndex)
        .forEach(({ code, line }) => extractValidNext(line, result, code));
      return;
    }
  };

  const availableNexts = (): ReturnType<WFMachine<CType>["availableNexts"]> => {
    const code = workflow.at(data.executionIndex);
    const result: ReturnType<WFMachine<CType>["availableNexts"]> = [];

    if (code) {
      extractValidNext(data.executionIndex, result, code);
    }

    availableTimeouts().forEach((timeout) => {
      const { name, control, actor } = timeout.ctimeout.consequence;
      result.push({
        name,
        actor,
        control,
        reason: "timeout",
      });
    });

    availableCompensateable().forEach((compensation) => {
      extractValidNext(
        compensation.compensationIndex,
        result,
        compensation.firstCompensation
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
        const lastEvent = getLastEventForParallel(evalContext.index - 1);
        // TODO: maybe support parallel at the beginning?
        // but I don't think that makes sense
        if (lastEvent === null) {
          throw new Error(
            "impossible right now. parallel should have been preceeded with a single event. this should have been prevented at the CCode building and validating"
          );
        }
        const newAtStack: SParallel<CType> = {
          t: "par",
          instances: [],
          lastEvent,
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

    const instanceCreatedCount = atStack.instances.length;
    const execDoneCount = atStack.instances.filter(
      (instance) => instance.entry.length >= evLength
    ).length;

    const maxCreatedReached = instanceCreatedCount >= maxCriteria;
    const minCompletedReached = execDoneCount >= minCriteria;
    const maxCompletedReached = execDoneCount >= maxCriteria;

    return {
      atStack,
      maxCreatedReached,
      minCompletedReached,
      maxCompletedReached,
      evLength,
      max,
      min,
    };
  };

  /**
   * Catch up with execution index
   */
  const recalculateResult = (evalContext: EvalContext) => {
    const calc = (): Continue => {
      const code = workflow.at(data.resultCalcIndex);
      const stackItem = data.stack.at(data.resultCalcIndex);
      if (code?.t === "event" && stackItem?.t === "event") {
        code.bindings?.forEach((x) => {
          const index = x.index;
          innerstate.context[x.var] = stackItem.event.payload.payload[index];
        });
        innerstate.state = [One, stackItem.event];

        if (code.control === "return") {
          innerstate.returned = true;
        }
      }

      if (code?.t === "anti-timeout" && stackItem?.t === "anti-timeout") {
        innerstate.state = [One, stackItem.event];

        const consequence = stackItem.consequence;
        if (consequence.control === Code.Control.fail) {
          const retryIndex = findRetryOnStack(data.resultCalcIndex);
          if (retryIndex === null) {
            throw new Error("cannot find retry while dealing with ");
          }
          resetIndex(evalContext, retryIndex + 1);
          return true;
        } else if (consequence.control === "return") {
          innerstate.returned = true;
        }
      }

      if (code?.t === "par" && stackItem?.t === "par") {
        innerstate.state = [
          Parallel,
          stackItem.lastEvent,
          stackItem.instances.map(
            (instance) => instance.entry[instance.entry.length - 1]
          ),
        ];
      }

      return false;
    };

    calc();

    while (data.resultCalcIndex < evalContext.index) {
      if (calc()) {
        continue;
      }

      data.resultCalcIndex += 1;
    }
  };

  type Continue = boolean;
  const Continue = true as const;
  type EvalContext = { index: number };
  const autoEvaluateImpl = (evalContext: EvalContext): Continue => {
    // Handle Retry Code
    const code = workflow.at(evalContext.index);
    if (!code) {
      innerstate.returned = true;
      return false;
    }

    if (code.t === "par") {
      readInnerParallelProcessInstances(evalContext, code);
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
        inner: WFMachine<CType>(code.subworkflow, multiverse),
      };
      if (atStack.t !== "match")
        throw new Error("match stack position filled with non-match");
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
              x.matchCase.case[1] === oneStateOrNull.meta.eventId)
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

  const autoEvaluate = (evalContext: EvalContext) => {
    while (true) {
      recalculateResult(evalContext);
      if (innerstate.returned) break;

      const shouldContinue = autoEvaluateImpl(evalContext);

      if (shouldContinue) continue;

      break;
    }
  };

  const tickTimeout = (e: ActyxWFBusiness<CType>): TickRes => {
    // TODO: there should not be multiple matches, shouldn't timeout be unique?
    const lastMatching = availableTimeouts()
      .filter((x) => x.ctimeout.consequence.name === e.payload.t)
      .at(0);

    if (lastMatching) {
      data.stack[lastMatching.antiIndex] = {
        t: "anti-timeout",
        consequence: lastMatching.ctimeout.consequence,
        event: e,
      };

      return { fed: true, jumpToIndex: lastMatching.antiIndex + 1 };
    }
    return { fed: false, jumpToIndex: null };
  };

  const feedEvent = (
    evalContext: EvalContext,
    code: CEvent<CType>,
    e: ActyxWFBusiness<CType>
  ) => {
    if (e.payload.t === code.name) {
      data.stack[evalContext.index] = {
        t: "event",
        event: e,
      };
      return true;
    }
    return false;
  };

  const tickChoice = (
    evalContext: EvalContext,
    code: CChoice,
    e: ActyxWFBusiness<CType>
  ): TickRes => {
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
      .find(([_, x]) => x.name === e.payload.t);

    if (firstMatching) {
      const [indexOffset, _] = firstMatching;
      const eventIndex = eventStartIndex + indexOffset;
      data.stack[eventIndex] = {
        t: "event",
        event: e,
      };
      return {
        fed: true,
        jumpToIndex: antiIndex,
      };
    }
    return {
      fed: false,
      jumpToIndex: null,
    };
  };

  /**
   * Explores the "children" of parallels
   */
  const readInnerParallelProcessInstances = (
    evalContext: EvalContext,
    parallelCode: CParallel
  ) => {
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
      atStack.instances.map((x) => x.entry[0].meta.eventId)
    );
    const newFirstEvents = multiverse
      .getNextById(atStack.lastEvent.meta.eventId)
      .filter(
        (ev) =>
          ev.payload.t === firstEventCode.name &&
          !registeredFirstEventIds.has(ev.meta.eventId)
      );

    newFirstEvents.forEach((e) => {
      const newInstance: SParallelExecution<CType> = {
        entry: [e],
      };
      atStack.instances.push(newInstance);
    });

    // Advance registered parallel instances
    atStack.instances.forEach((parallelStack) => {
      while (true) {
        const index = parallelStack.entry.length;
        if (index >= evLength) return;
        const code = workflow.at(index);
        if (code?.t !== "event") return;
        const last =
          parallelStack.entry[parallelStack.entry.length - 1].meta.eventId;
        const validNext = multiverse
          .getNextById(last)
          .filter((ev) => ev.payload.t === code.name);
        const sortedNext = sortByEventKey(validNext).at(0);
        if (!sortedNext) return;
        parallelStack.entry.push(sortedNext);
      }
    });
  };

  const tickParallel = (
    evalContext: EvalContext,
    parallelCode: CParallel,
    input: TickInput
  ): TickRes => {
    let jumpToIndex: number | null = null;
    let fed = false;

    const { atStack, minCompletedReached, maxCompletedReached } =
      fetchParallelCriteria(evalContext, parallelCode);

    // advance signal from predecessors

    if (minCompletedReached) {
      // Can advance, how to advance? It depends if `min` is 0 or not.
      const nextEvalContext = { index: atStack.nextEvalIndex };
      // move nextEvalIndex as far as it can
      autoEvaluate(nextEvalContext);

      fed = tickAt(nextEvalContext, input);
      atStack.nextEvalIndex = nextEvalContext.index;

      if (fed) {
        jumpToIndex = Math.max(
          atStack.nextEvalIndex,
          evalContext.index + parallelCode.pairOffsetIndex + 1
        );
      }
    }

    if (!fed && maxCompletedReached) {
      jumpToIndex = Math.max(
        atStack.nextEvalIndex,
        evalContext.index + parallelCode.pairOffsetIndex + 1
      );
    }

    return {
      fed,
      jumpToIndex: jumpToIndex,
    };
  };

  const tickCompensation = (e: TickInput): TickRes => {
    // Compensation can only be triggered by event, not seek

    const firstMatching = availableCompensateable().at(0);

    if (firstMatching) {
      const firstMatchingIndex = firstMatching.firstCompensationIndex;
      active.activeCompensation.delete(firstMatching.compensationIndex);
      const fed = feedEvent(
        { index: firstMatching.firstCompensationIndex },
        firstMatching.firstCompensation,
        e
      );
      if (fed) {
        return { fed, jumpToIndex: firstMatchingIndex };
      }
    }

    return { fed: false, jumpToIndex: null };
  };

  const tickMatch = (
    evalContext: EvalContext,
    _: CMatch<CType>,
    e: TickInput
  ): TickRes => {
    const atStack = getSMatchAtIndex(evalContext.index);
    if (!atStack) {
      throw new Error("missing match at stack on evaluation");
    }
    data.stack[evalContext.index] = atStack;
    const fed = atStack.inner.tick(e);
    return { fed, jumpToIndex: null };
  };

  const tickRest = (
    evalContext: EvalContext,
    code: CItem<CType>,
    e: TickInput
  ): TickRes => {
    const fed = (() => {
      if (code?.t === "event") return feedEvent(evalContext, code, e);
      return false;
    })();

    return {
      fed,
      jumpToIndex: fed ? evalContext.index + 1 : null,
    };
  };

  const id = `id:${Math.round(Math.random() * 1000)}`;

  const tickAt = (evalContext: EvalContext, e: TickInput | null): boolean => {
    let fed = false;
    const code = workflow.at(evalContext.index);
    if (e) {
      if (code) {
        const res = (() => {
          // Jumps
          // =========
          const compRes = tickCompensation(e);
          if (compRes.fed) return compRes;

          const timeoutRes = tickTimeout(e);
          if (timeoutRes.fed) return timeoutRes;

          // Non Jumps
          // =========
          if (code?.t === "match") return tickMatch(evalContext, code, e);
          if (code?.t === "choice") return tickChoice(evalContext, code, e);
          if (code?.t === "par") return tickParallel(evalContext, code, e);
          return tickRest(evalContext, code, e);
        })();

        fed = res.fed;

        if (res?.jumpToIndex) {
          evalContext.index = res.jumpToIndex;
        }
      }
    }

    autoEvaluate(evalContext);

    return fed;
  };

  const tick = (input: TickInput | null): boolean => {
    let evalContext = { index: data.executionIndex };
    const fed = tickAt(evalContext, input);
    data.executionIndex = evalContext.index;
    return fed;
  };

  const state = (): WFMachineState<CType> => {
    const evalContext = { index: data.executionIndex };
    const atStack = getSMatchAtIndex(evalContext.index);
    if (atStack) return atStack.inner.state();

    return {
      state: innerstate.state,
      context: innerstate.context,
    };
  };

  const getLatestStateEvent = () => {
    const s = state().state;
    if (!s) return null;
    return s[1];
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

    active.activeCompensation = new Set();
    active.activeTimeout = new Set();

    innerstate.context = {};
    innerstate.state = null;
    innerstate.returned = false;
  };

  const advanceToMostCanon = () => {
    while (true) {
      // get all valid nexts
      // parallel and compensation may lead to dead ends.
      const validNextNames = new Set(
        availableNexts()
          .filter((x) => x.reason !== "parallel" && x.reason !== "compensation")
          .map((x) => x.name)
      );

      const allNextEvents = (() => {
        const last = getLatestStateEvent();
        if (!last) return multiverse.getRoots();
        return sortByEventKey(multiverse.getNextById(last.meta.eventId));
      })().filter((x) => validNextNames.has(x.payload.t));

      const next = sortByEventKey(allNextEvents).at(0);

      if (next) {
        const fed = tick(next);
        if (fed === false) return;
      } else {
        tick(null);
        return;
      }
    }
  };

  const advanceToEventId = (eventId: string) => {
    const continuationChain = (() => {
      const point = multiverse.getById(eventId);
      // given event ID is invalid
      if (!point) return null;

      const chain = createLinearChain(multiverse, point);
      const latestEvent = getLatestStateEvent();
      if (!latestEvent) {
        return chain;
      } else {
        const lastProcessedEventIndex = chain.findIndex(
          (x) => x.meta.eventId === latestEvent.meta.eventId
        );
        // means that this machine is already off the path
        if (lastProcessedEventIndex === -1) return null;

        return chain.slice(lastProcessedEventIndex + 1);
      }
    })();

    if (!continuationChain || continuationChain.length === 0) {
      tick(null);
    } else {
      while (continuationChain.length > 0) {
        const first = continuationChain.shift();
        if (!first) break;
        tick(first);
      }
    }
  };

  const resetAndAdvanceToEventId = (eventId: string) => {
    reset();
    advanceToEventId(eventId);
  };

  const self: WFMachine<CType> = {
    tick,
    state,
    latestStateEvent: getLatestStateEvent,
    returned,
    availableTimeouts,
    availableNexts,
    availableCompensateable: () =>
      availableCompensateable().map((x) => ({
        codeIndex: x.codeIndex,
        fromTimelineOf: x.fromTimelineOf,
        name: x.firstCompensation.name,
        actor: x.firstCompensation.actor,
      })),
    advanceToMostCanon,
    resetAndAdvanceToMostCanon: () => {
      reset();
      advanceToMostCanon();
    },
    resetAndAdvanceToEventId,
  };

  return self;
};
