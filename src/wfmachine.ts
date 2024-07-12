/**
 * @module WFMachine
 *
 * WFMachine is given a WFWorkflow and a reference to the multiverse tree.
 *
 * It pointers that:
 * - traverses around points within the WFWorkflow code.
 * - traverses around event point in the multiverse tree.
 *
 * It is not network-aware. It relies on having an external party:
 * - append events to the tree.
 * - call its methods e.g. `advanceToMostCanon`, `resetAndAdvanceToEventId`, and
 *   `reset`.
 */

import {
  ActyxWFBusiness,
  CTypeProto,
  NestedCodeIndexAddress,
  sortByEventKey,
} from "./consts.js";
import { createLinearChain } from "./event-utils.js";
import { MultiverseTree } from "./multiverse.js";
import { Logger, makeLogger, Ord } from "./utils.js";
import {
  Actor,
  CAntiParallel,
  CAntiRetry,
  CAntiTimeout,
  CChoice,
  CCompensate,
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
  Unique,
  WFWorkflow,
  validateBindings,
} from "./wfcode.js";

/**
 * StackItem are markers created in the WFMachine corresponding to the
 * WFWorkflow code. Not all WFWorkflow code type needs its StackItem
 * counterpart.
 *
 * The significance of one type of StackItem is different from another. That can
 * be examined in the `autoEvaluateImpl` function.
 */
type StackItem<CType extends CTypeProto> =
  | SCompensate<CType>
  | SMatch<CType>
  | SEvent<CType>
  | SRetry
  | STimeout<CType>
  | SAntiTimeout<CType>
  | SParallel<CType>
  | SAntiParallel;

type SCompensate<CType extends CTypeProto> = Pick<CCompensate, "t"> & {
  lastEvent: ActyxWFBusiness<CType>;
};

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

/**
 * A state can be One or Parallel.
 */
type State<CType extends CTypeProto> =
  | One<ActyxWFBusiness<CType>>
  | Parallel<ActyxWFBusiness<CType>>;

export type One<T = unknown> = [typeof One, T];

/**
 * Parallel state, apart from the tag, contains 2 parts:
 * - The first `T` is the triggering event, the last event before the beginning of the `PARALLEL` block.
 * - The second `T[]` is the states of parallel event sequences that stems from the triggering event.
 */
export type Parallel<T = unknown> = [typeof Parallel, T, T[]];

export type WFMachineState<CType extends CTypeProto> = {
  state: State<CType> | null;
  /**
   * Context here maps binding -> identity.
   */
  context: Record<string, unknown>;
};

/**
 * A "next" is the next event that can be fed into the machine.
 * If the Actor is a Unique, the value is the unique binding's bound identity.
 */
type Next<CType extends CTypeProto> = {
  actor: Actor<CType>;
  name: CType["ev"];
  control?: Code.Control;
  reason: null | "compensation" | "timeout" | "parallel";
};

/**
 * An "unnamed next" is the next event that can be fed into the machine.
 * If the Actor is a unique, the value is a unique binding name.
 */
type UnnamedNext<CType extends CTypeProto> = {
  unnamedActor: Actor<CType>;
  name: CType["ev"];
  control?: Code.Control;
  reason: null | "compensation" | "timeout" | "parallel";
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
    codeIndex: NestedCodeIndexAddress.Type;
    actor: Actor<CType>;
    name: CType["ev"];
    fromTimelineOf: string;
  }[];
  availableNexts: () => Next<CType>[];
  advanceToMostCanon: () => void;
  resetAndAdvanceToMostCanon: () => void;
  resetAndAdvanceToEventId: (eventId: string) => void;
  logger: Logger;
};

/**
 * Constructor for WFMachine
 */
export const WFMachine = <CType extends CTypeProto>(
  wfWorkflow: WFWorkflow<CType>,
  multiverse: MultiverseTree.Type<CType>,
  wfMachineArgs?: {
    context?: Record<string, unknown>;
    codeIndexPrefix: NestedCodeIndexAddress.Type;
  }
): WFMachine<CType> => {
  const contextArg = wfMachineArgs?.context || {};
  const wfMachineCodeIndexPrefix = wfMachineArgs?.codeIndexPrefix || [];
  type EvalContext = { index: number };
  type TickInput = ActyxWFBusiness<CType>;
  type TickRes = { jumpToIndex: number | null; fed: boolean };

  validateBindings(wfWorkflow);

  const logger = makeLogger();
  const workflow = wfWorkflow.code;
  const ccompensateIndexer = CCompensationIndexer.make(workflow);
  const cparallelIndexer = CParallelIndexer.make(workflow);

  const mapUniqueActorOnNext = (res: UnnamedNext<CType>): Next<CType> => ({
    ...res,
    actor: (() => {
      if (res.unnamedActor.t === "Unique") {
        return Unique(innerstate.context[res.unnamedActor.get()]);
      }
      return res.unnamedActor;
    })(),
  });

  // pointers and stack
  const data = {
    extraParCalcIndex: 0 as number | null, // does not support nested parallel
    stateCalcIndex: 0,
    evalIndex: 0,
    stack: [] as (StackItem<CType> | null)[],
  };

  /**
   * Sets of indices of compensation and timeout that are active.
   */
  const active = {
    compensateable: new Set() as Set<number>,
    timeout: new Set() as Set<number>,
  };

  // state-related data
  const innerstate = {
    context: { ...contextArg } as Record<string, unknown>,
    state: null as State<CType> | null,
    stateIndex: -1 as number,
    returned: false,
  };

  const resetIndex = (evalContext: EvalContext, targetIndex: number) => {
    // set execution back at the index
    data.stack.length = targetIndex;
    data.evalIndex = targetIndex;
    evalContext.index = targetIndex;

    // remove timeouts after the last item index in stack
    active.timeout = new Set(
      Array.from(active.timeout).filter((index) => index >= data.stack.length)
    );

    // force recalculate context
    data.stateCalcIndex = 0;
    data.extraParCalcIndex = null; // does not support nested parallel
    innerstate.context = { ...contextArg };
    innerstate.state = null;
    innerstate.stateIndex = -1;
  };

  // Finders helpers

  const nullifyMatchingTimeout = (
    antiTimeout: CAntiTimeout,
    antiTimeoutIndex: number
  ) => {
    const timeoutIndex = antiTimeoutIndex + antiTimeout.pairOffsetIndex;
    const maybeTimeout = data.stack.at(timeoutIndex);

    if (active.timeout.has(timeoutIndex) && maybeTimeout?.t === "timeout") {
      active.timeout.delete(timeoutIndex);
      active.timeout.delete(timeoutIndex);
      data.stack[timeoutIndex] = null;
    } else {
      throw new Error(
        `timeout not found on stack at ${timeoutIndex} to ${antiTimeoutIndex}`
      );
    }
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
    Array.from(active.timeout)
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
  const selfAvailableCompensateable = () => {
    const res = Array.from(active.compensateable)
      .map((index) => {
        const ccompensate = workflow.at(index);
        if (ccompensate?.t !== "compensate") {
          throw new Error(
            `compensate query fatal error: ccompensate not found at index ${index}`
          );
        }

        const sCompensate = data.stack.at(index);
        if (sCompensate?.t !== "compensate") {
          throw new Error(
            `compensate at ${index} not populated when activated at index`
          );
        }

        const compensationIndex = index;
        const triggeringEvent = sCompensate.lastEvent;
        const withIndex = index + ccompensate.withIndexOffset;
        const antiIndex = index + ccompensate.antiIndexOffset;
        const firstCompensationIndex = index + ccompensate.withIndexOffset + 1;
        const firstCompensation = workflow.at(firstCompensationIndex);

        if (firstCompensation?.t !== "event") {
          throw new Error(
            `compensate query fatal error: compensation.with first code is not of type event`
          );
        }

        return {
          codeIndex: wfMachineCodeIndexPrefix.concat([index]),
          compensationIndex,
          code: ccompensate,
          firstCompensation,
          firstCompensationIndex,
          antiTimeoutIndex: antiIndex,
          withIndex,
          fromTimelineOf: triggeringEvent.meta.eventId,
        };
      })
      .filter((x): x is Exclude<typeof x, null> => x !== null)
      .sort((a, b) => {
        // in case of nested compensations: sort by last/innermost timeout
        return b.compensationIndex - a.compensationIndex;
      });

    return res;
  };

  /**
   * Extract valid next event types from a code.
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

      const parStack = generateParallelCriteria({ index }, code).atStack;
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
      const smatch = data.stack.at(data.evalIndex);
      if (smatch && smatch.t === "match") {
        result.push(...smatch.inner.availableNexts());
      }
      return;
    }

    if (code?.t === "event") {
      const { name, control } = code;
      result.push(
        mapUniqueActorOnNext({
          name,
          unnamedActor: code.actor,
          control,
          reason: (() => {
            if (ccompensateIndexer.isInsideWithBlock(index))
              return "compensation";
            if (cparallelIndexer.isParallelStart(index)) return "parallel";
            return null;
          })(),
        })
      );
      return;
    }

    if (code?.t === "choice") {
      const eventStartIndex = data.evalIndex + 1;
      const antiIndex = data.evalIndex + code.antiIndexOffset;
      workflow
        .map((code, line) => ({ code, line }))
        .slice(eventStartIndex, antiIndex)
        .forEach(({ code, line }) => extractValidNext(line, result, code));
      return;
    }
  };

  /**
   * Extract Next[] from current state
   */
  const availableNexts = (): ReturnType<WFMachine<CType>["availableNexts"]> => {
    const code = workflow.at(data.evalIndex);
    const result: ReturnType<WFMachine<CType>["availableNexts"]> = [];

    if (code) {
      extractValidNext(data.evalIndex, result, code);
    }

    availableTimeouts().forEach((timeout) => {
      const { name, control, actor } = timeout.ctimeout.consequence;
      result.push(
        mapUniqueActorOnNext({
          name,
          unnamedActor: actor,
          control,
          reason: "timeout",
        })
      );
    });

    selfAvailableCompensateable().forEach((compensation) => {
      extractValidNext(
        compensation.firstCompensationIndex,
        result,
        compensation.firstCompensation
      );
    });

    return result;
  };

  /**
   * Parallel criteria is various criteria that decides how an event is
   * processed when the the WFMachine is in Parallel state.
   */
  const generateParallelCriteria = (
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
  const recalculateState = (evalContext: EvalContext) => {
    const calc = (calcIndex: number): Continue => {
      const code = workflow.at(calcIndex);
      const stackItem = data.stack.at(calcIndex);

      if (code?.t === "event" && stackItem?.t === "event") {
        code.bindings?.forEach((x) => {
          const index = x.index;
          innerstate.context[x.var] = stackItem.event.payload.payload[index];
        });
        innerstate.state = [One, stackItem.event];
        innerstate.stateIndex = calcIndex;

        if (code.control === "return") {
          innerstate.returned = true;
        }
      }

      if (code?.t === "anti-timeout" && stackItem?.t === "anti-timeout") {
        const consequence = stackItem.consequence;

        if (consequence.control === Code.Control.fail) {
          const retryIndex = findRetryOnStack(calcIndex);
          if (retryIndex === null) {
            throw new Error("cannot find retry while dealing with ");
          }
          resetIndex(evalContext, retryIndex + 1);
          return true;
        } else if (consequence.control === "return") {
          innerstate.state = [One, stackItem.event];
          innerstate.returned = true;
        }
      }

      if (code?.t === "par" && stackItem?.t === "par") {
        innerstate.state = (() => {
          const instances = stackItem.instances.map(
            (instance) => instance.entry[instance.entry.length - 1]
          );
          if (instances.length === 0) {
            return [One, stackItem.lastEvent];
          } else {
            return [Parallel, stackItem.lastEvent, instances];
          }
        })();
      }

      return false;
    };

    // `stateCalcIndex` needs to be rolled back to the last `stateIndex` to make
    // sure that no updates are missed. (this has been an issue with events
    // inside `compensate-with` blocks)
    data.stateCalcIndex = Math.min(data.stateCalcIndex, innerstate.stateIndex);

    while (data.stateCalcIndex < evalContext.index) {
      if (calc(data.stateCalcIndex)) {
        continue;
      }
      data.stateCalcIndex += 1;
    }

    // special case for parallel
    if (
      data.extraParCalcIndex !== null &&
      data.extraParCalcIndex === evalContext.index
    ) {
      calc(data.extraParCalcIndex);
    }
  };

  type Continue = boolean;
  const Continue = true as const;

  const autoEvaluateImpl = (evalContext: EvalContext): Continue => {
    // Handle Retry Code
    const code = workflow.at(evalContext.index);

    if (!code) {
      innerstate.returned = true;
      return false;
    }

    if (code.t === "par") {
      readInnerParallelProcessInstances(evalContext, code);
      data.extraParCalcIndex = evalContext.index;
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
      data.stack[evalContext.index] = {
        t: "timeout",
        startedAt: new Date(),
      };
      active.timeout.add(evalContext.index);
      evalContext.index += 1;
      return true;
    }

    if (code.t === "anti-timeout") {
      nullifyMatchingTimeout(code, evalContext.index);
      evalContext.index += 1;
      return true;
    }

    if (code.t === "compensate") {
      const lastEvent = innerstate.state?.[1];
      if (!lastEvent) throw new Error("entered compensate block without state");

      active.compensateable.add(evalContext.index);
      data.stack[evalContext.index] = {
        t: "compensate",
        lastEvent,
      };

      evalContext.index += 1;
      return true;
    }

    if (code.t === "compensate-end") {
      const originalCompensateIndex = evalContext.index + code.baseIndexOffset;
      active.compensateable.delete(originalCompensateIndex);

      const rightAfterAntiIndex = evalContext.index + code.antiIndexOffset + 1;
      evalContext.index = rightAfterAntiIndex;
      return true;
    }

    if (code.t === "anti-compensate") {
      const originalCompensateIndex = evalContext.index + code.baseIndexOffset;
      active.compensateable.delete(originalCompensateIndex);

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
      const context = {} as Record<string, unknown>;
      Object.entries(code.args).forEach(([assignee, assigner]) => {
        context[assignee] = innerstate.context[assigner];
      });
      const matchMachine = WFMachine<CType>(code.subworkflow, multiverse, {
        context,
        codeIndexPrefix: [evalContext.index],
      });
      matchMachine.logger.sub(logger.log);
      const atStack = getSMatchAtIndex(evalContext.index) || {
        t: "match",
        inner: matchMachine,
      };
      if (atStack.t !== "match")
        throw new Error("match stack position filled with non-match");
      data.stack[evalContext.index] = atStack;
      if (!atStack.inner.returned()) return false;

      // calculate returned
      const { state } = atStack.inner.state();
      if (!state) throw new Error("returned without state");
      const returnedState = state[1].payload.t;

      const cases = code.casesIndexOffsets.map((offset) => {
        const index = evalContext.index + offset;
        const matchCase = workflow.at(index);
        if (matchCase?.t !== "match-case") {
          throw new Error(
            `case index offset points to the wrong code type: ${matchCase?.t}`
          );
        }
        return { offset, matchCase };
      });

      // cases that will not match otherwise
      const notOtherwises = new Set(
        cases
          .map((x) => {
            if (x.matchCase.case[0] === Exact) return x.matchCase.case[1];
            return false;
          })
          .filter((x): x is Exclude<typeof x, null> => x !== null)
      );

      const firstMatch = cases.find((c) => {
        if (c.matchCase.case[0] === Exact) {
          return c.matchCase.case[1] === returnedState;
        }
        return !notOtherwises.has(returnedState);
      });

      if (!firstMatch) {
        throw new Error(`no case matches for ${returnedState} at ${code}`);
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

  /**
   * Some `CItem` auto-moves to the next one, instead of waiting for an input
   * like `CEvent` or `CParallel`.
   *
   * This `autoEvaluate` does this auto-moving.
   */
  const autoEvaluate = (evalContext: EvalContext) => {
    while (true) {
      recalculateState(evalContext);
      if (innerstate.returned) break;

      const shouldContinue = autoEvaluateImpl(evalContext);

      if (shouldContinue) continue;

      break;
    }
    recalculateState(evalContext);
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
    const { atStack, evLength } = generateParallelCriteria(
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
      generateParallelCriteria(evalContext, parallelCode);

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
    const firstMatching = selfAvailableCompensateable().find(
      (comp) => e.payload.t === comp.firstCompensation.name
    );

    if (firstMatching) {
      const fed = feedEvent(
        { index: firstMatching.firstCompensationIndex },
        firstMatching.firstCompensation,
        e
      );
      if (fed) {
        return { fed, jumpToIndex: firstMatching.firstCompensationIndex + 1 };
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

  const tickAt = (evalContext: EvalContext, e: TickInput | null): boolean => {
    let fed = false;
    const code = workflow.at(evalContext.index);
    if (e) {
      if (code) {
        const res = (() => {
          // Submachine
          // =========
          const matchRes =
            code?.t === "match" && tickMatch(evalContext, code, e);
          if (matchRes && matchRes.fed) return matchRes;

          // Jumps
          // =========
          const compRes = tickCompensation(e);
          if (compRes.fed) return compRes;

          const timeoutRes = tickTimeout(e);
          if (timeoutRes.fed) return timeoutRes;

          // Non Jumps
          // =========
          if (code?.t === "choice") return tickChoice(evalContext, code, e);
          if (code?.t === "par") return tickParallel(evalContext, code, e);
          return tickRest(evalContext, code, e);
        })();

        fed = res.fed;

        if (res !== null && res.jumpToIndex !== null) {
          evalContext.index = res.jumpToIndex;
        }
      }
    }

    autoEvaluate(evalContext);

    return fed;
  };

  /**
   * Feeds an input or a null to the machine and update the `data.evalIndex`.
   * See the rest of `tick*` functions for how it works.
   */
  const tick = (input: TickInput | null): boolean => {
    let evalContext = { index: data.evalIndex };
    const fed = tickAt(evalContext, input);
    data.evalIndex = evalContext.index;
    return fed;
  };

  const state = (): WFMachineState<CType> => {
    const evalContext = { index: data.evalIndex };
    const atStack = getSMatchAtIndex(evalContext.index);
    if (atStack) {
      const state = atStack.inner.state();
      if (state.state) {
        return state;
      }
    }

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
    const evalContext = { index: data.evalIndex };
    const atStack = getSMatchAtIndex(evalContext.index);
    if (atStack) return atStack.inner.returned();

    return innerstate.returned;
  };

  const reset = () => {
    data.evalIndex = 0;
    // calc index
    data.stateCalcIndex = 0;
    data.extraParCalcIndex = null;
    data.stack = [];

    active.timeout = new Set();

    innerstate.context = { ...contextArg };
    innerstate.state = null;
    innerstate.stateIndex = -1;
    innerstate.returned = false;
  };

  /**
   * From the point in the multiverse, indicated by the eventId of the
   * WFMachine's state, go to the winning future (using actyx's EventKey
   * sorting) as far as possible.
   */
  const advanceToMostCanon = () => {
    while (true) {
      // get all valid nexts
      // parallel and compensation may lead to dead ends.
      const validNextNames = new Set(
        availableNexts()
          .filter((x) => x.reason !== "parallel")
          .map((x) => x.name)
      );

      const allNextEvents = (() => {
        const last = getLatestStateEvent();
        if (!last) return multiverse.getRoots();
        const sorted = sortByEventKey(
          multiverse.getNextById(last.meta.eventId)
        );

        return sorted;
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

  /**
   * Using a reference point, generate a history and apply this to the machine.
   */
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

  /**
   * Reset before advancing to eventId
   */
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
    availableCompensateable: () => {
      const res = selfAvailableCompensateable().map((x) => ({
        codeIndex: x.codeIndex,
        fromTimelineOf: x.fromTimelineOf,
        name: x.firstCompensation.name,
        actor: x.firstCompensation.actor,
      }));

      const match = getSMatchAtIndex(data.evalIndex);
      if (match) {
        res.push(...match.inner.availableCompensateable());
      }

      res.sort((a, b) => {
        const res = Ord.toNum(
          NestedCodeIndexAddress.cmp(b.codeIndex, a.codeIndex)
        );
        return res;
      });

      return res;
    },
    advanceToMostCanon,
    resetAndAdvanceToMostCanon: () => {
      reset();
      advanceToMostCanon();
    },
    resetAndAdvanceToEventId,
    logger,
  };

  return self;
};
