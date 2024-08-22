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
 *
 * WFMachine operates in two different level:
 * - advance* - which tells the WFMachine to advance through the point in
 *   multiverse
 * - tick() - which tells the WFMachine to move once
 *
 * Within a tick, WFMachine goes through two steps:
 * - tick* - which, on the existence of an input, pattern-match the input to the
 *   code pointed by evalIndex, and suggest a jump to the evalIndex.
 * - autoEvaluate - which calculates the state and moves the evalIndex
 *   automatically when possible, in an interlaced manner. It is IMPORTANT that
 *   `tick*` does not cause any side effect the pointers and only "suggest"
 *   jumps to the evalIndex and keep the responsibility of evalIndex movements
 *   to autoEvaluate.
 *   - State calculation has a separate pointer from autoEvaluate called
 *     stateCalcIndex. stateCalcIndex will always follow evalIndex but not
 *     match. Its value will always be lower than evalIndex.
 */

import {
  ActyxWFBusiness,
  CTypeProto,
  NestedCodeIndexAddress,
  sortByEventKey,
  WFMarkerCanonAdvrt,
} from "./consts.js";
import { createLinearChain } from "./event-utils.js";
import { CanonizationStore, MultiverseTree } from "./multiverse.js";
import { Logger, makeLogger, Ord } from "./utils.js";
import {
  Actor,
  CAntiCompensate,
  CAntiParallel,
  CCanonize,
  CChoice,
  CCompensate,
  CCompensationIndexer,
  CEvent,
  CItem,
  CMatch,
  CParallel,
  CParallelIndexer,
  CRetry,
  CRetryIndexer,
  CTimeout,
  CTimeoutIndexer,
  Code,
  Exact,
  Unique,
  WFWorkflow,
  validate,
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
  | SCanonize<CType>
  | SCompensate<CType>
  | SAntiCompensate
  | SMatch<CType>
  | SEvent<CType>
  | SRetry<CType>
  | STimeout
  | SParallel<CType>
  | SAntiParallel;

type SCanonize<CType extends CTypeProto> = Pick<CCanonize, "t"> & {
  lastEvent: ActyxWFBusiness<CType>;
};

type SCompensate<CType extends CTypeProto> = Pick<CCompensate, "t"> & {
  lastEvent: ActyxWFBusiness<CType>;
};

/**
 * a marker to say that a compensation is done
 */
type SAntiCompensate = Pick<CAntiCompensate, "t">;

type SMatch<CType extends CTypeProto> = Pick<CMatch<CType>, "t"> & {
  inner: WFMachine<CType>;
};
type SEvent<CType extends CTypeProto> = Pick<CEvent<CType>, "t"> & {
  event: ActyxWFBusiness<CType>;
};

type SRetry<CType extends CTypeProto> = Pick<CRetry, "t"> & {
  lastState: State<CType> | null;
};
type SParallel<CType extends CTypeProto> = Pick<CParallel, "t"> & {
  lastEvent: ActyxWFBusiness<CType>;
  nextEvalIndex: number;
  instances: SParallelExecution<CType>[];
};
type SParallelExecution<CType extends CTypeProto> = {
  entry: ActyxWFBusiness<CType>[];
};

type STimeout = Pick<CTimeout, "t"> & {
  startedAt: Date;
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

type ReasonSet = Set<null | "compensation" | "timeout" | "parallel">;

/**
 * A "next" is the next event that can be fed into the machine.
 * If the Actor is a Unique, the value is the unique binding's bound identity.
 */
type Next<CType extends CTypeProto> = {
  actor: Actor<CType>;
  name: CType["ev"];
  control?: Code.Control;
  reason: ReasonSet;
};

/**
 * An "unnamed next" is the next event that can be fed into the machine.
 * If the Actor is a unique, the value is a unique binding name.
 */
type UnnamedNext<CType extends CTypeProto> = {
  unnamedActor: Actor<CType>;
  name: CType["ev"];
  control?: Code.Control;
  reason: ReasonSet;
};

type PendingCanonizationAdvert<CType extends CTypeProto> = Omit<
  WFMarkerCanonAdvrt<CType>,
  "ax" | "advertiser"
>;

export type WFMachine<CType extends CTypeProto> = {
  tick: (input: ActyxWFBusiness<CType> | null) => boolean;
  state: () => WFMachineState<CType>;
  latestStateEvent: () => ActyxWFBusiness<CType> | null;
  returned: () => boolean;
  availableTimeouts: () => {
    codeIndex: NestedCodeIndexAddress.Type;
    ctimeout: CTimeout;
    cconsequence: CEvent<CType>;
    lateness: number;
  }[];
  availableCompensateable: () => {
    codeIndex: NestedCodeIndexAddress.Type;
    firstCompensation: CEvent<CType>;
    fromTimelineOf: string;
    involvedActors: string[];
  }[];
  activeCompensation: () => {
    codeIndex: NestedCodeIndexAddress.Type;
    firstCompensation: CEvent<CType>;
    fromTimelineOf: string;
    involvedActors: string[];
  }[];
  doneCompensation: () => {
    codeIndex: NestedCodeIndexAddress.Type;
    fromTimelineOf: string;
  }[];
  isWaitingForCanonization: () => boolean;
  pendingCanonizationAdvrt: () => PendingCanonizationAdvert<CType> | null;
  availableNexts: () => Next<CType>[];
  advanceToMostCanon: () => void;
  resetAndAdvanceToMostCanon: () => void;
  resetAndAdvanceToEventId: (eventId: string) => void;
  logger: Logger;
};

export type SwarmData<CType extends CTypeProto> = {
  readonly multiverseTree: MultiverseTree.Type<CType>;
  readonly canonizationStore: CanonizationStore.Type<CType>;
};

/**
 * Constructor for WFMachine
 */
export const WFMachine = <CType extends CTypeProto>(
  wfWorkflow: WFWorkflow<CType>,
  swarmStore: SwarmData<CType>,
  wfMachineArgs?: {
    context?: Record<string, string>;
    codeIndexPrefix: NestedCodeIndexAddress.Type;
  }
): WFMachine<CType> => {
  const { multiverseTree: multiverse } = swarmStore;
  const contextArg = wfMachineArgs?.context || {};
  const wfMachineCodeIndexPrefix = wfMachineArgs?.codeIndexPrefix || [];
  type EvalContext = { index: number };
  type TickInput = ActyxWFBusiness<CType>;
  type TickRes = { jumpToIndex: number | null; fed: boolean };

  validate(wfWorkflow);

  const logger = makeLogger();
  const workflow = wfWorkflow.code;
  const ccompensateIndexer = CCompensationIndexer.make(workflow);
  const cparallelIndexer = CParallelIndexer.make(workflow);
  const ctimeoutIndexer = CTimeoutIndexer.make(workflow);
  const cretryIndexer = CRetryIndexer.make(workflow);

  const mapUniqueActorOnNext = (res: UnnamedNext<CType>): Next<CType> => ({
    control: res.control,
    name: res.name,
    reason: res.reason,
    actor: (() => {
      if (res.unnamedActor.t === "Unique") {
        return Unique(innerstate.context[res.unnamedActor.get()]);
      }
      return res.unnamedActor;
    })(),
  });

  // pointers and stack
  const data = {
    stateCalcIndex: 0,
    evalIndex: 0,
    stack: [] as (StackItem<CType> | null)[],
  };

  // state-related data
  const innerstate = {
    context: { ...contextArg } as Record<string, string>,
    state: null as State<CType> | null,
    returned: false,
  };

  const resetIndex = (evalContext: EvalContext, targetIndex: number) => {
    // set execution back at the index
    data.stack.length = targetIndex;
    data.evalIndex = targetIndex;
    evalContext.index = targetIndex;

    // force recalculate context
    data.stateCalcIndex = 0;
    innerstate.context = { ...contextArg };
    innerstate.state = null;
  };

  const findRetryOnStack = (indexInput: number) => {
    const closest = cretryIndexer
      .getListMatching(indexInput)
      .sort((a, b) => b.start - a.start)
      .at(0);

    if (!closest) return null;
    const index = closest.start;

    const code = workflow.at(index);
    if (code?.t !== "retry") return null;
    const stack = data.stack.at(index);
    if (stack?.t !== "retry") return null;

    return { code, stack, index };
  };

  const getSMatchAtIndex = (index: number) => {
    const atStack = data.stack.at(index);
    if (atStack?.t !== "match") return null;
    return atStack;
  };

  /**
   * Find all active timeouts
   */
  const selfAvailableTimeouts = (evalIndex: number = data.evalIndex) =>
    ctimeoutIndexer
      .getListMatching(evalIndex)
      .map((entry) => entry.start)
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
            `timeout query fatal error: stimeout not found at index ${index} while inspecting ${data.evalIndex}. s shape is ${stimeout}`
          );
        }

        const gapIndex = index + ctimeout.gapOffsetIndex;
        const antiIndex = index + ctimeout.antiOffsetIndex;
        const cconsquenceIndex = gapIndex + 1;

        if (evalIndex >= gapIndex) return null;

        const cconsequence = workflow.at(cconsquenceIndex);

        if (cconsequence?.t !== "event") {
          throw new Error(
            `timeout query fatal error: cevent consequence not found at index ${cconsquenceIndex} while inspecting ${data.evalIndex}.`
          );
        }

        const dueDate = stimeout.startedAt.getTime() + ctimeout.duration;
        const lateness = Date.now() - dueDate;

        return {
          codeIndex: wfMachineCodeIndexPrefix.concat([index]),
          stimeout,
          ctimeout,
          cconsquenceIndex,
          cconsequence,
          lateness,
          antiIndex,
        };
      })
      .filter((x): x is NonNullable<typeof x> => !!x)
      .sort((a, b) => {
        // in case of nested timeouts: sorted by last / outermost timeout
        return b.antiIndex - a.antiIndex;
      });

  const selfDoneCompensation = () =>
    data.stack
      .map((x, i) => [i, x, workflow.at(i)] as const)
      .filter(
        (pair): pair is [number, SAntiCompensate, CAntiCompensate] =>
          pair[1]?.t === "anti-compensate" && pair[2]?.t === "anti-compensate"
      )
      .map(([antiIndex, _, cAntiCompensate]) => {
        const compensateIndex = antiIndex + cAntiCompensate.baseIndexOffset;
        const ccompensate = workflow.at(compensateIndex);
        if (ccompensate?.t !== "compensate") {
          throw new Error(
            `compensate query fatal error: ccompensate not found at index ${compensateIndex}`
          );
        }

        const sCompensate = data.stack.at(compensateIndex);
        if (sCompensate?.t !== "compensate") {
          throw new Error(
            `compensate at ${compensateIndex} not populated when activated at index`
          );
        }

        const triggeringEvent = sCompensate.lastEvent;

        return {
          codeIndex: wfMachineCodeIndexPrefix.concat([compensateIndex]),
          fromTimelineOf: triggeringEvent.meta.eventId,
        };
      });

  const involvedActorsOfCompensateAt = (index: number) =>
    (ccompensateIndexer.involvementMap.get(index) || [])
      .map((binding) => innerstate.context[binding] || undefined)
      .filter((x): x is string => !!x);

  const selfActiveCompensation = (evalIndex = data.evalIndex) => {
    const res = ccompensateIndexer
      .getWithListMatching(evalIndex)
      .map((entry) => {
        const withStart = workflow.at(entry.start);
        if (withStart?.t !== "compensate-with") {
          return null;
        }
        const compensateIndex = entry.start + withStart.baseIndexOffset;
        const ccompensate = workflow.at(compensateIndex);
        if (ccompensate?.t !== "compensate") {
          throw new Error(
            `compensate query fatal error: ccompensate not found at index ${compensateIndex}`
          );
        }

        const sCompensate = data.stack.at(compensateIndex);
        if (sCompensate?.t !== "compensate") {
          throw new Error(
            `compensate at ${compensateIndex} not populated when activated at index`
          );
        }

        const triggeringEvent = sCompensate.lastEvent;
        const firstCompensationIndex =
          compensateIndex + ccompensate.withIndexOffset + 1;
        const firstCompensation = workflow.at(firstCompensationIndex);

        if (firstCompensation?.t !== "event") {
          throw new Error(
            `compensate query fatal error: compensation.with first code is not of type event`
          );
        }

        return {
          codeIndex: wfMachineCodeIndexPrefix.concat([compensateIndex]),
          firstCompensation,
          firstCompensationIndex,
          fromTimelineOf: triggeringEvent.meta.eventId,
          involvedActors: involvedActorsOfCompensateAt(compensateIndex),
        };
      })
      .filter((x): x is Exclude<typeof x, null> => x !== null);

    return res;
  };

  /**
   * Find all active compensateable
   */
  const selfAvailableCompensateable = (evalIndex = data.evalIndex) => {
    const res = ccompensateIndexer
      .getMainListMatching(evalIndex)
      .map((entry) => entry.start)
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

        const triggeringEvent = sCompensate.lastEvent;
        const firstCompensationIndex = index + ccompensate.withIndexOffset + 1;
        const firstCompensation = workflow.at(firstCompensationIndex);

        if (firstCompensation?.t !== "event") {
          throw new Error(
            `compensate query fatal error: compensation.with first code is not of type event`
          );
        }

        return {
          codeIndex: wfMachineCodeIndexPrefix.concat([index]),
          firstCompensation,
          firstCompensationIndex,
          fromTimelineOf: triggeringEvent.meta.eventId,
          involvedActors: involvedActorsOfCompensateAt(index),
        };
      })
      .filter((x): x is Exclude<typeof x, null> => x !== null);

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
        const childCode = workflow.at(childIndex);
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
        const subNexts = smatch.inner.availableNexts();
        if (ccompensateIndexer.isInsideWithBlock(index)) {
          subNexts.forEach((next) => next.reason.add("compensation"));
        }
        result.push(...subNexts);
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
          reason: ((): ReasonSet => {
            if (ccompensateIndexer.isInsideWithBlock(index))
              return new Set(["compensation"]);
            if (cparallelIndexer.isParallelStart(index))
              return new Set(["parallel"]);
            return new Set();
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

    selfAvailableTimeouts(data.evalIndex).forEach((timeout) => {
      const { name, control, actor } = timeout.cconsequence;
      result.push(
        mapUniqueActorOnNext({
          name,
          unnamedActor: actor,
          control,
          reason: new Set(["timeout"]),
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
      if (atStack?.t !== "par") {
        throw new Error(`stack type at ${evalContext.index} not par`);
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
          const actorId = stackItem.event.payload.payload[index];
          if (typeof actorId !== "string") {
            throw new Error("actorname is not string");
          }
          innerstate.context[x.var] = actorId;
        });
        innerstate.state = [One, stackItem.event];

        if (code.control === Code.Control.return) {
          innerstate.returned = true;
        } else if (code.control === Code.Control.fail) {
          const retry = findRetryOnStack(calcIndex);
          if (retry === null) {
            throw new Error("cannot find retry while dealing with fail event");
          }
          data.stack[retry.index] = {
            t: "retry",
            lastState: innerstate.state,
          };
          resetIndex(evalContext, retry.index + 1);
          return true;
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

      if (
        code?.t === "retry" &&
        stackItem?.t === "retry" &&
        stackItem.lastState !== null
      ) {
        innerstate.state = stackItem.lastState;
      }

      if (code?.t === "match") {
        const inner = getSMatchAtIndex(calcIndex)?.inner;
        if (inner && inner.returned()) {
          innerstate.state = inner.state().state;
        }
      }

      return false;
    };

    while (data.stateCalcIndex < evalContext.index) {
      if (data.stateCalcIndex >= 0) {
        if (calc(data.stateCalcIndex)) {
          continue;
        }
      }
      data.stateCalcIndex += 1;
    }

    // special case for parallel
    if (data.stack.at(evalContext.index)?.t === "par") {
      calc(evalContext.index);
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

    if (code.t === "canonize") {
      const atStack = (() => {
        const atStack = data.stack.at(evalContext.index);
        if (!atStack) {
          const lastEvent = innerstate.state?.[1] || null;
          if (!lastEvent) {
            throw new Error("canonize is impossible before any event");
          }
          const newAtStack: SCanonize<CType> = { t: "canonize", lastEvent };
          data.stack[evalContext.index] = newAtStack;
          return newAtStack;
        }
        if (atStack.t === "canonize") {
          return atStack;
        }
        throw new Error(`atStack ${evalContext.index} is not "canonize"`);
      })();

      // check for existing forTimeline
      const name = atStack.lastEvent.payload.t;
      const timelineOf = atStack.lastEvent.meta.eventId;
      const depth = multiverse.depthOf(atStack.lastEvent.meta.eventId);

      const matchingCanonizeEvent = swarmStore.canonizationStore
        .getDecisionsForAddress(name, depth)
        .find((x) => x.payload.timelineOf === timelineOf);

      // resume
      if (matchingCanonizeEvent) {
        evalContext.index += 1;
        return true;
      }
    }

    if (code.t === "par") {
      // initialize atStack
      (() => {
        const atStack = data.stack.at(evalContext.index);
        if (!atStack) {
          const lastEvent = innerstate.state?.[1] || null;
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
            nextEvalIndex: evalContext.index + code.pairOffsetIndex + 1,
          };
          data.stack[evalContext.index] = newAtStack;
          return;
        }
        if (atStack?.t !== "par") {
          throw new Error("stack type not par");
        }
      })();

      readInnerParallelProcessInstances(evalContext, code);

      // recursive autoEvaluate on "par" next
      const { atStack, minCompletedReached } = generateParallelCriteria(
        evalContext,
        code
      );
      if (minCompletedReached) {
        const nextEvalContext = { index: atStack.nextEvalIndex };
        autoEvaluate(nextEvalContext);
        atStack.nextEvalIndex = nextEvalContext.index;
      }

      return false;
    }

    if (code.t === "retry") {
      data.stack[evalContext.index] = {
        t: "retry",
        lastState: innerstate.state,
      };
      evalContext.index += 1;
      return true;
    }

    if (code.t === "anti-retry") {
      const pairIndex = code.pairOffsetIndex + evalContext.index;
      if (workflow.at(pairIndex)?.t !== "retry") {
        throw new Error("retry not found");
      }

      data.stack[pairIndex] = null;
      evalContext.index += 1;
      return true;
    }

    // Handle timeout code

    if (code.t === "timeout") {
      data.stack[evalContext.index] = {
        t: "timeout",
        startedAt: new Date(),
      };
      evalContext.index += 1;
      return true;
    }

    if (code.t === "timeout-gap") {
      evalContext.index += code.antiOffsetIndex + 1;
      return true;
    }

    if (code.t === "anti-timeout") {
      return false;
    }

    if (code.t === "compensate") {
      const lastEvent = innerstate.state?.[1];
      if (!lastEvent) throw new Error("entered compensate block without state");

      data.stack[evalContext.index] = {
        t: "compensate",
        lastEvent,
      };

      evalContext.index += 1;
      return true;
    }

    if (code.t === "compensate-end") {
      const rightAfterAntiIndex = evalContext.index + code.antiIndexOffset + 1;
      evalContext.index = rightAfterAntiIndex;
      return true;
    }

    if (code.t === "anti-compensate") {
      if (data.stack.at(evalContext.index) === undefined) {
        data.stack[evalContext.index] = { t: "anti-compensate" };
      }
      return false;
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
      const context = {} as Record<string, string>;
      Object.entries(code.args).forEach(([assignee, assigner]) => {
        context[assignee] = innerstate.context[assigner];
      });
      const matchMachine = WFMachine<CType>(code.subworkflow, swarmStore, {
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

  const tickTimeout = (
    evalContext: EvalContext,
    e: ActyxWFBusiness<CType>
  ): TickRes => {
    const lastMatching = selfAvailableTimeouts(evalContext.index).find(
      (x) => x.cconsequence.name === e.payload.t
    );

    if (lastMatching) {
      const { cconsquenceIndex, cconsequence } = lastMatching;
      const fed = feedEvent({ index: cconsquenceIndex }, cconsequence, e);
      if (fed) {
        return { fed: true, jumpToIndex: cconsquenceIndex + 1 };
      }
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
        if (x.t === "choice-barrier") return null;
        if (x.t !== "event") {
          // defensive measure, should not exist
          throw new Event("codes inside are not CEvent");
        }
        return [index, x] as const;
      })
      .filter((x): x is NonNullable<typeof x> => x !== null)
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

  const tickCompensation = (
    evalContext: EvalContext,
    e: TickInput
  ): TickRes => {
    // Compensation can only be triggered by event, not seek
    const firstMatching = selfAvailableCompensateable(evalContext.index).find(
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
          const compRes = tickCompensation(evalContext, e);
          if (compRes.fed) return compRes;

          const timeoutRes = tickTimeout(evalContext, e);
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

    const matchAtStack = getSMatchAtIndex(evalContext.index);
    if (matchAtStack) {
      const state = matchAtStack.inner.state();
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
    data.stack.length = 0;

    innerstate.context = { ...contextArg };
    innerstate.state = null;
    innerstate.returned = false;
  };

  const getContinuationChainByEventId = (eventId: string) => {
    const latestEvent = getLatestStateEvent();
    const point = multiverse.getById(eventId);
    // given event ID is invalid
    if (!point) return null;

    const chain = createLinearChain(multiverse, point);

    if (!latestEvent) {
      return chain;
    } else {
      const lastProcessedEventIndex = chain.findIndex(
        (x) => x.meta.eventId === latestEvent.meta.eventId
      );

      if (lastProcessedEventIndex === -1) return null;

      return chain.slice(lastProcessedEventIndex + 1);
    }
  };

  /**
   * From the point in the multiverse, indicated by the eventId of the
   * WFMachine's state, go to the winning future (using actyx's EventKey
   * sorting) as far as possible.
   */
  const advanceToMostCanon = () => {
    let tickHasRun = false;

    // force-choose timeline from canonization store
    const continuationChain = (() => {
      const decisions = swarmStore.canonizationStore.listDecisionsFromLatest();
      if (decisions.length === 0) return null;

      for (const decision of decisions) {
        const historyWhereInvolved = getContinuationChainByEventId(
          decision.payload.timelineOf
        );

        if (historyWhereInvolved) {
          return historyWhereInvolved;
        }
      }

      return null;
    })();

    // playback historic continuation chain
    if (continuationChain) {
      while (continuationChain.length > 0) {
        const first = continuationChain.shift();
        if (!first) break;
        tick(first);
        tickHasRun = true;
      }
    }

    // forward mode
    while (true) {
      // get all valid nexts
      // parallel and compensation may lead to dead ends.
      const validNextNames = new Set(
        availableNexts()
          .filter((x) => !x.reason.has("parallel"))
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
        tickHasRun = true;
        if (fed === false) return;
      } else {
        if (!tickHasRun) tick(null);
        return;
      }
    }
  };

  /**
   * Using a reference point, generate a history and apply this to the machine.
   */
  const advanceToEventId = (eventId: string) => {
    const continuationChain = getContinuationChainByEventId(eventId);

    if (!continuationChain || continuationChain.length === 0) {
      tick(null);
      return;
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

  const selfPendingCanonizationAdvrt =
    (): PendingCanonizationAdvert<CType> | null => {
      const code = workflow.at(data.evalIndex);
      const atStack = data.stack.at(data.evalIndex);

      if (code?.t === "canonize" && atStack?.t === "canonize") {
        const {
          meta: { eventId: timelineOf },
          payload: { t: stateName },
        } = atStack.lastEvent;

        const depth = multiverse.depthOf(atStack.lastEvent.meta.eventId);

        const existingAdvrt = swarmStore.canonizationStore
          .getAdvertisementsForAddress(stateName, depth)
          .find((req) => req.payload.timelineOf === timelineOf);

        if (!existingAdvrt) {
          const uniqueActorDesignation = code.actor.get();
          const canonizer = innerstate.context[uniqueActorDesignation];

          return { canonizer, name: stateName, depth, timelineOf };
        }
      }

      return null;
    };

  const isWaitingForCanonization = () => {
    const code = workflow.at(data.evalIndex);
    if (code?.t !== "canonize") return false;
    const atStack = data.stack.at(data.evalIndex);
    if (atStack?.t !== "canonize") return false;
    const name = atStack.lastEvent.payload.t;
    const depth = multiverse.depthOf(atStack.lastEvent.meta.eventId);

    return (
      swarmStore.canonizationStore.getDecisionsForAddress(name, depth)
        .length === 0
    );
  };

  const self: WFMachine<CType> = {
    tick,
    state,
    latestStateEvent: getLatestStateEvent,
    returned,
    availableTimeouts: () => {
      const res = selfAvailableTimeouts().map(
        ({ ctimeout, cconsequence, lateness, codeIndex }) => ({
          codeIndex,
          ctimeout,
          cconsequence,
          lateness,
        })
      );

      const match = getSMatchAtIndex(data.evalIndex);
      if (match) {
        const inner = match.inner.availableTimeouts();
        res.push(...inner);
      }

      res.sort((a, b) => {
        return Ord.toNum(NestedCodeIndexAddress.cmp(b.codeIndex, a.codeIndex));
      });

      return res;
    },
    availableNexts,
    availableCompensateable: () => {
      const res = selfAvailableCompensateable().map(
        ({ codeIndex, firstCompensation, fromTimelineOf, involvedActors }) => ({
          codeIndex,
          firstCompensation,
          fromTimelineOf,
          involvedActors,
        })
      );

      const match = getSMatchAtIndex(data.evalIndex);
      if (match) {
        const inner = match.inner.availableCompensateable();
        res.push(...inner);
      }

      res.sort((a, b) => {
        return Ord.toNum(NestedCodeIndexAddress.cmp(b.codeIndex, a.codeIndex));
      });

      return res;
    },
    activeCompensation: () => {
      const res = selfActiveCompensation().map(
        ({ codeIndex, firstCompensation, fromTimelineOf, involvedActors }) => ({
          codeIndex,
          firstCompensation,
          fromTimelineOf,
          involvedActors,
        })
      );

      const match = getSMatchAtIndex(data.evalIndex);
      if (match) {
        const inner = match.inner.activeCompensation();
        res.push(...inner);
      }

      res.sort((a, b) => {
        return Ord.toNum(NestedCodeIndexAddress.cmp(b.codeIndex, a.codeIndex));
      });

      return res;
    },
    doneCompensation: () => {
      const res = selfDoneCompensation();
      const match = getSMatchAtIndex(data.evalIndex);
      if (match) {
        const inner = match.inner.doneCompensation();
        res.push(...inner);
      }

      res.sort((a, b) => {
        return Ord.toNum(NestedCodeIndexAddress.cmp(a.codeIndex, b.codeIndex));
      });

      return res;
    },
    isWaitingForCanonization,
    pendingCanonizationAdvrt: () =>
      selfPendingCanonizationAdvrt() ||
      getSMatchAtIndex(data.evalIndex)?.inner.pendingCanonizationAdvrt() ||
      null,
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
