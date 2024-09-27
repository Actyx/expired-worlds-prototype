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

import { isNonNullChain } from "typescript";
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
  CEvent,
  CItem,
  CMatch,
  CParallel,
  CRetry,
  CTimeout,
  Code,
  CodeGraph,
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
  | STimeout<CType>
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
  lastFailState: ActyxWFBusiness<CType> | null;
};
type SParallel<CType extends CTypeProto> = Pick<CParallel, "t"> & {
  nextLegIndex: number;
  lastEvent: ActyxWFBusiness<CType>;
  instances: SParallelExecution<CType>[];
};
type SParallelExecution<CType extends CTypeProto> = {
  entry: ActyxWFBusiness<CType>[];
};

type STimeout<CType extends CTypeProto> = Pick<CTimeout, "t"> & {
  startedAt: Date;
  lastTimeoutEvent: ActyxWFBusiness<CType> | null;
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

export type BranchOfWithNamedActor = {
  chain: number[];
  terminating: boolean;
  involved: Set<string>;
};

export const convertBranchOfToNamed = <CType extends CTypeProto>(
  context: Record<string, string>,
  branch: CodeGraph.BranchOf<CType>
): BranchOfWithNamedActor => {
  const names = new Set(
    branch.involved
      .map((x) => {
        if (x.t === "Unique") {
          const uniqueRoleName = x.get();
          const name = context[uniqueRoleName] || null;
          if (name) {
            return [name];
          }
        }
        return [];
      })
      .flatMap((x) => x)
  );

  return {
    chain: branch.chain,
    terminating: branch.terminating,
    involved: names,
  };
};

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
    fromTimelineOf: string;
    involvedActors: string[];
  }[];
  activeCompensation: () => {
    codeIndex: NestedCodeIndexAddress.Type;
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
  codegraph: () => CodeGraph.Type<CType>;
  codegraphBranchesFromCompensation: () => null | BranchOfWithNamedActor[];
  isInvolved: (identity: { role: CType["role"]; id: string }) => boolean;
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
  compilerCache: CodeGraph.CompilerCache<CType>,
  wfMachineArgs?: {
    context?: Record<string, string>;
    codeIndexPrefix: NestedCodeIndexAddress.Type;
  }
): WFMachine<CType> => {
  const { multiverseTree: multiverse } = swarmStore;
  const contextArg = wfMachineArgs?.context || {};
  const wfMachineCodeIndexPrefix = wfMachineArgs?.codeIndexPrefix || [];
  type TickContext = { legIndex: number };
  type TickInput = ActyxWFBusiness<CType>;
  type TickRes = { jumpLeg: number | null; fed: boolean };

  validate(wfWorkflow);

  const logger = makeLogger();
  const workflow = wfWorkflow.code;
  const ccompensateIndexer = compilerCache.indexer.compensation.get(workflow);
  const cparallelIndexer = compilerCache.indexer.parallel.get(workflow);
  const ctimeoutIndexer = compilerCache.indexer.timeout.get(workflow);
  const cretryIndexer = compilerCache.indexer.retry.get(workflow);
  const codegraph = compilerCache.codegraph.get(workflow);

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
    legIndex: -1,
    stack: [] as (StackItem<CType> | null)[],
  };

  // state-related data
  const innerstate = {
    stateCalcIndex: 0,
    context: { ...contextArg } as Record<string, string>,
    state: null as State<CType> | null,
  };

  const resetRecalc = () => {
    innerstate.stateCalcIndex = -1;
    innerstate.context = { ...contextArg };
    innerstate.state = null;
  };

  const resetIndex = (tickContext: TickContext, targetIndex: number) => {
    // set execution back at the index
    data.stack.length = targetIndex;
    data.legIndex = -1;
    tickContext.legIndex = targetIndex;
  };

  const getSMatchAtIndex = (index: number) => {
    const atStack = data.stack.at(index);
    if (atStack?.t !== "match") return null;
    return atStack;
  };

  /**
   * Find all active timeouts
   */
  const selfAvailableTimeouts = (legIndex: number = data.legIndex) =>
    (codegraph.nextMap.at(legIndex) || [])
      .map((next) => {
        if (next.code.t === "canonize") return null;
        next = next as CodeGraph.NextCEvent<CType>;
        const timeoutTag = next.tags.timeoutJump;
        if (!timeoutTag) return null;
        const timeoutIndex = timeoutTag.timeoutIndex;
        const ctimeout = workflow.at(timeoutTag.timeoutIndex);
        if (ctimeout?.t !== "timeout") {
          throw new Error(
            `attempt timeout fatal error: ctimeout not found at index ${timeoutIndex}`
          );
        }
        const stimeout = data.stack.at(timeoutIndex);
        if (stimeout?.t !== "timeout") {
          throw new Error(
            `timeout query fatal error: stimeout not found at index ${timeoutIndex} while inspecting ${legIndex}. s shape is ${stimeout}`
          );
        }

        const antiIndex = timeoutIndex + ctimeout.antiOffsetIndex;
        const cconsequenceIndex = next.index;
        const cconsequence = next.code;

        const dueDate = stimeout.startedAt.getTime() + ctimeout.duration;
        const lateness = Date.now() - dueDate;

        return {
          codeIndex: wfMachineCodeIndexPrefix.concat([timeoutIndex]),
          stimeout,
          ctimeout,
          cconsequenceIndex,
          cconsequence,
          lateness,
          antiIndex,
          codeNext: next,
        };
      })
      .filter((x): x is NonNullable<typeof x> => !!x)
      .sort((a, b) => {
        // in case of nested timeouts: sorted by last / outermost timeout
        return b.antiIndex - a.antiIndex;
      });

  const selfDoneCompensation = (
    legIndex: number = data.legIndex
  ): {
    codeIndex: NestedCodeIndexAddress.Type;
    fromTimelineOf: string;
  }[] => {
    const anti = workflow.at(legIndex);
    if (anti?.t !== "anti-compensate") return [];
    const compensateIndex = legIndex + anti.baseIndexOffset;
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
    return [
      {
        codeIndex: wfMachineCodeIndexPrefix.concat([compensateIndex]),
        fromTimelineOf: triggeringEvent.meta.eventId,
      },
    ];
  };

  const involvedActorsOfCompensateAt = (index: number) =>
    (ccompensateIndexer.involvementMap.get(index) || [])
      .map((binding) => innerstate.context[binding] || undefined)
      .filter((x): x is string => !!x);

  const selfActiveCompensation = (legIndex = data.legIndex) => {
    if (returned()) return [];

    const res = ccompensateIndexer
      .getWithListMatching(legIndex)
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
        const withIndex = compensateIndex + ccompensate.withIndexOffset;
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
          withIndex,
        };
      })
      .filter((x): x is Exclude<typeof x, null> => x !== null);

    return res;
  };

  /**
   * Find all active compensateable
   */
  const selfAvailableCompensateable = (legIndex = data.legIndex) =>
    (codegraph.nextMap.at(legIndex) || [])
      .map((next) => {
        if (next.code.t === "canonize") return null;
        next = next as CodeGraph.NextCEvent<CType>;
        const compensateableTag = next.tags.compensationEntry;
        if (!compensateableTag) return null;

        const index = compensateableTag.compensationIndex;
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
        const withIndex = index + ccompensate.withIndexOffset;
        const firstCompensationIndex = next.index;
        const firstCompensation = next.code;

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
          next,
          withIndex,
        };
      })
      .filter((x): x is Exclude<typeof x, null> => x !== null);

  const availableNexts = (): ReturnType<WFMachine<CType>["availableNexts"]> => {
    const uniqueCheck = new Set<string>();
    const convertCodeNextToWFMachineNext = (
      codeNexts: CodeGraph.Nexts<CType>
    ): UnnamedNext<CType>[] =>
      codeNexts
        .map((next) => {
          if (next.code.t === "canonize") return null;
          next = next as CodeGraph.NextCEvent<CType>;
          return {
            unnamedActor: next.code.actor,
            name: next.code.name,
            control: next.code.control,
            reason: (() => {
              const set: ReasonSet = new Set();
              if (next.tags.compensationEntry) set.add("compensation");
              if (next.tags.parallelContinuation) set.add("parallel");
              if (next.tags.parallelInstantiation) set.add("parallel");
              if (next.tags.timeoutJump) set.add("timeout");
              return set;
            })(),
          };
        })
        .filter((x): x is Exclude<typeof x, null> => x !== null)
        .filter((x) => {
          if (uniqueCheck.has(x.name)) return false;
          uniqueCheck.add(x.name);
          return true;
        });

    if (data.legIndex === -1) {
      return convertCodeNextToWFMachineNext(codegraph.baseNexts).map(
        mapUniqueActorOnNext
      );
    } else {
      const code = workflow[data.legIndex];

      if (code.t === "par") {
        const parallelCriteria = generateParallelCriteria(
          { legIndex: data.legIndex },
          code
        );
        const nextLegIndex = parallelCriteria.atStack.nextLegIndex;
        const codeNexts = [
          // from nexts
          ...(codegraph.nextMap.at(nextLegIndex) || []),
          // from children
          ...parallelCriteria.atStack.instances.flatMap((instance) => {
            const childIndex =
              data.legIndex + code.firstEventIndex + instance.entry.length - 1;
            const codeNexts = codegraph.nextMap.at(childIndex) || [];
            return codeNexts;
          }),
        ];

        return convertCodeNextToWFMachineNext(codeNexts).map(
          mapUniqueActorOnNext
        );
      } else if (code.t === "match") {
        const inner = getSMatchAtIndex(data.legIndex)?.inner;
        if (!inner) return [];
        return inner.availableNexts();
      } else {
        return convertCodeNextToWFMachineNext(
          codegraph.nextMap.at(data.legIndex) || []
        ).map(mapUniqueActorOnNext);
      }
    }
  };

  /**
   * Parallel criteria is various criteria that decides how an event is
   * processed when the the WFMachine is in Parallel state.
   */
  const generateParallelCriteria = (
    tickContext: TickContext,
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
      const atStack = data.stack.at(tickContext.legIndex);
      if (atStack?.t !== "par") {
        throw new Error(
          `par at ${tickContext.legIndex} is not created during AssignAhead`
        );
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
  const recalculateState = (tickContext: TickContext) => {
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

        return true;
      }

      if (code?.t === "par" && stackItem?.t === "par") {
        innerstate.state = [
          Parallel,
          stackItem.lastEvent,
          stackItem.instances.map(
            (instance) => instance.entry[instance.entry.length - 1]
          ),
        ];

        return true;
      }

      if (
        code?.t === "retry" &&
        stackItem?.t === "retry" &&
        stackItem.lastFailState !== null
      ) {
        innerstate.state = [One, stackItem.lastFailState];

        return true;
      }

      return false;
    };

    if (innerstate.stateCalcIndex > tickContext.legIndex) {
      resetRecalc();
    }

    while (innerstate.stateCalcIndex < tickContext.legIndex) {
      if (innerstate.stateCalcIndex >= 0) {
        calc(innerstate.stateCalcIndex);
      }
      innerstate.stateCalcIndex++;
    }

    // special case for parallel
    if (data.stack.at(tickContext.legIndex)?.t === "par") {
      calc(tickContext.legIndex);
    }
  };

  type Continue = boolean;
  const Continue = true as const;

  const autoEvaluate = (tickContext: TickContext) => {
    const legSlide = (tickContext: TickContext) => {
      const legSlide = codegraph.legSlideMap.at(tickContext.legIndex);
      if (legSlide !== undefined) {
        tickContext.legIndex = legSlide;
        return Continue;
      }
      return !Continue;
    };

    const tryAdvanceCanonize = (tickContext: TickContext) => {
      const next = selfNextCanonize();
      if (next) {
        const atStack = data.stack.at(next.index);
        if (atStack?.t !== "canonize") {
          throw new Error(
            "fatal error canonize is not populated by AssignAhead mechanism"
          );
        }
        // check for existing forTimeline
        const name = atStack.lastEvent.payload.t;
        const timelineOf = atStack.lastEvent.meta.eventId;
        const depth = multiverse.depthOf(atStack.lastEvent.meta.eventId);

        const matchingCanonizeEvent = swarmStore.canonizationStore
          .getDecisionsForAddress(name, depth)
          .find((x) => x.payload.timelineOf === timelineOf);

        // resume
        if (matchingCanonizeEvent) {
          tickContext.legIndex = next.index;
          return Continue;
        }
      }
      return !Continue;
    };

    const tryAdvanceMatch = (tickContext: TickContext) => {
      const code = workflow.at(tickContext.legIndex);
      if (code?.t === "match") {
        const atStack =
          getSMatchAtIndex(tickContext.legIndex) ||
          (() => {
            const context = {} as Record<string, string>;
            Object.entries(code.args).forEach(([assignee, assigner]) => {
              context[assignee] = innerstate.context[assigner];
            });
            const matchMachine = WFMachine<CType>(
              code.subworkflow,
              swarmStore,
              compilerCache,
              {
                context,
                codeIndexPrefix: [tickContext.legIndex],
              }
            );
            matchMachine.logger.sub(logger.log);
            return {
              t: "match",
              inner: matchMachine,
            };
          })();

        if (atStack.t !== "match") {
          throw new Error("match stack position filled with non-match");
        }

        data.stack[tickContext.legIndex] = atStack;
        if (!atStack.inner.returned()) return !Continue;

        // calculate returned
        const { state } = atStack.inner.state();
        if (!state) throw new Error("returned without state");
        const returnedState = state[1].payload.t;

        const cases = code.casesIndexOffsets.map((offset) => {
          const index = tickContext.legIndex + offset;
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

        tickContext.legIndex += firstMatch.offset;
        return Continue;
      }

      return !Continue;
    };

    const tryAdvancePar = (tickContext: TickContext) => {
      const code = workflow.at(tickContext.legIndex);
      if (code?.t === "par") {
        readInnerParallelProcessInstances(tickContext, code);

        // recursive autoEvaluate on "par" next
        const { atStack, minCompletedReached } = generateParallelCriteria(
          tickContext,
          code
        );
        if (minCompletedReached) {
          const nextEvalContext: TickContext = {
            legIndex: atStack.nextLegIndex,
          };
          autoEvaluate(nextEvalContext);
          atStack.nextLegIndex = nextEvalContext.legIndex;
        }
      }
      return !Continue;
    };

    while (true) {
      recalculateState(tickContext);
      const shouldContinue =
        legSlide(tickContext) ||
        tryAdvanceCanonize(tickContext) ||
        tryAdvanceMatch(tickContext) ||
        tryAdvancePar(tickContext);

      if (!shouldContinue) break;
    }
    recalculateState(tickContext);
  };

  const tickTimeout = (
    tickContext: TickContext,
    e: ActyxWFBusiness<CType>
  ): TickRes => {
    const timeout = selfAvailableTimeouts(tickContext.legIndex).find(
      (timeout) => timeout.cconsequence.name === e.payload.t
    );

    if (timeout && feedEvent(timeout.codeNext, e)) {
      return { fed: true, jumpLeg: timeout.cconsequenceIndex };
    }

    return { fed: false, jumpLeg: null };
  };

  const patchStateOnFeedEvent = (
    next: CodeGraph.NextCEvent<CType>,
    e: ActyxWFBusiness<CType>
  ) => {
    // Patch State on Feed
    if (next.tags.retryPatch) {
      data.stack[next.tags.retryPatch.retryIndex] = {
        t: "retry",
        lastFailState: getSelfStateOrThrow(
          "feedEvent: retryPatch state not found"
        ),
      } satisfies SRetry<CType>;
    }

    if (next.tags.compensationMainEntry) {
      data.stack[next.tags.compensationMainEntry.compensationIndex] = {
        t: "compensate",
        lastEvent: getSelfStateOrThrow(
          "feedEvent: compensationMainEntry state not found"
        ),
      } satisfies SCompensate<CType>;
    }

    if (next.tags.timeoutEntry) {
      const timeoutCode = workflow.at(next.tags.timeoutEntry.timeoutIndex);
      if (timeoutCode?.t !== "timeout") {
        throw new Error("invalid timeout index while feedEvent:timeoutEntry");
      }
      data.stack[next.tags.timeoutEntry.timeoutIndex] = {
        t: "timeout",
        startedAt: new Date(),
        lastTimeoutEvent: null,
      } satisfies STimeout<CType>;
    } else if (next.tags.timeoutJump) {
      const timeoutCode = workflow.at(next.tags.timeoutJump.timeoutIndex);
      if (timeoutCode?.t !== "timeout") {
        throw new Error("invalid timeout index while feedEvent:timeoutJump");
      }
      data.stack[next.tags.timeoutJump.timeoutIndex] = {
        t: "timeout",
        startedAt: new Date(),
        lastTimeoutEvent: e,
      } satisfies STimeout<CType>;
    }

    // patch AssignAhead for parallel and canonize
    codegraph.assignAheadsMap
      .at(next.index)
      ?.forEach(({ index: assignAheadIndex }) => {
        const code = workflow.at(assignAheadIndex);
        if (code?.t === "par") {
          data.stack[assignAheadIndex] = {
            t: "par",
            instances: [],
            lastEvent: e,
            nextLegIndex: assignAheadIndex + code.pairOffsetIndex,
          } satisfies SParallel<CType>;
        }
        if (code?.t === "canonize") {
          data.stack[assignAheadIndex] = {
            t: "canonize",
            lastEvent: e,
          } satisfies SCanonize<CType>;
        }
      });
  };

  const feedEvent = (
    next: CodeGraph.NextCEvent<CType>,
    e: ActyxWFBusiness<CType>
  ) => {
    if (e.payload.t === next.code.name) {
      data.stack[next.index] = {
        t: "event",
        event: e,
      };
      patchStateOnFeedEvent(next, e);
      return true;
    }
    return false;
  };

  /**
   * Explores the "children" of parallels
   */
  const readInnerParallelProcessInstances = (
    tickContext: TickContext,
    parallelCode: CParallel
  ) => {
    const { atStack, evLength } = generateParallelCriteria(
      tickContext,
      parallelCode
    );

    // New instances detection
    const firstEventCode = workflow.at(
      tickContext.legIndex + parallelCode.firstEventIndex
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
    tickContext: TickContext,
    parallelCode: CParallel,
    e: TickInput
  ): TickRes => {
    let jumpLeg: number | null = null;
    let fed = false;

    const { atStack, minCompletedReached, maxCompletedReached } =
      generateParallelCriteria(tickContext, parallelCode);

    // advance signal from predecessors
    if (minCompletedReached) {
      const nextTickContext = { legIndex: atStack.nextLegIndex };
      if (tickAt(nextTickContext, e)) {
        jumpLeg = atStack.nextLegIndex;
      }
      atStack.nextLegIndex = nextTickContext.legIndex;
    }

    if (maxCompletedReached) {
      jumpLeg = atStack.nextLegIndex;
    }

    return { fed, jumpLeg };
  };

  const tickCompensation = (
    tickContext: TickContext,
    e: TickInput
  ): TickRes => {
    const compensation = selfAvailableCompensateable(tickContext.legIndex).find(
      (comp) => comp.firstCompensation.name === e.payload.t
    );

    // Compensation can only be triggered by event, not seek
    if (compensation && feedEvent(compensation.next, e)) {
      return { fed: true, jumpLeg: compensation.firstCompensationIndex + 1 };
    }

    return { fed: false, jumpLeg: null };
  };

  const tickMatch = (
    tickContext: TickContext,
    _: CMatch<CType>,
    e: TickInput
  ): TickRes => {
    const atStack = getSMatchAtIndex(tickContext.legIndex);
    if (!atStack) {
      throw new Error("missing match at stack on evaluation");
    }
    data.stack[tickContext.legIndex] = atStack;
    const fed = atStack.inner.tick(e);
    return { fed, jumpLeg: null };
  };

  const tickRest = (tickContext: TickContext, e: TickInput): TickRes => {
    const next = (codegraph.nextMap.at(tickContext.legIndex) || [])
      .filter((x): x is CodeGraph.NextCEvent<CType> => x.code.t !== "canonize")
      .find((next) => {
        return (
          !next.tags.parallelInstantiation &&
          !next.tags.parallelContinuation &&
          next.code.name === e.payload.t
        );
      });

    if (next && feedEvent(next, e)) {
      return { fed: true, jumpLeg: next.index };
    }

    return { fed: false, jumpLeg: null };
  };

  const tickAt = (tickContext: TickContext, e: TickInput | null): boolean => {
    let fed = false;
    const code = workflow.at(tickContext.legIndex);

    if (e) {
      if (code) {
        const res = (() => {
          // Jumps
          // =========
          const compRes = tickCompensation(tickContext, e);
          if (compRes.fed) return compRes;

          const timeoutRes = tickTimeout(tickContext, e);
          if (timeoutRes.fed) return timeoutRes;

          // Submachine
          // =========
          const matchRes =
            code?.t === "match" && tickMatch(tickContext, code, e);
          if (matchRes && matchRes.fed) return matchRes;

          // Non Jumps
          // =========
          if (code?.t === "par") return tickParallel(tickContext, code, e);
          return tickRest(tickContext, e);
        })();

        fed = res.fed;

        if (res !== null && res.jumpLeg !== null) {
          tickContext.legIndex = res.jumpLeg;
        }
      }
    }

    autoEvaluate(tickContext);

    return fed;
  };

  /**
   * Feeds an input or a null to the machine and update the `data.evalIndex`.
   * See the rest of `tick*` functions for how it works.
   */
  const tick = (input: TickInput | null): boolean => {
    let tickContext: TickContext = { legIndex: data.legIndex };
    const fed = tickAt(tickContext, input);
    data.legIndex = tickContext.legIndex;
    return fed;
  };

  const selfState = () => ({
    state: innerstate.state,
    context: innerstate.context,
  });

  const getSelfStateOrThrow = (reason: string) => {
    const state = selfState().state?.[1];
    if (!state) {
      throw new Error(reason);
    }
    return state;
  };

  const state = (): WFMachineState<CType> => {
    const tickContext = { index: data.legIndex };

    const matchAtStack = getSMatchAtIndex(tickContext.index);
    if (matchAtStack) {
      const state = matchAtStack.inner.state();
      if (state.state) {
        return state;
      }
    }

    return selfState();
  };

  const getLatestStateEvent = () => {
    const s = state().state;
    if (!s) return null;
    return s[1];
  };

  const returned = () => codegraph.terminuses.has(data.legIndex);

  const reset = () => {
    // calc index
    data.legIndex = -1;
    data.stack.length = 0;

    innerstate.stateCalcIndex = 0;
    innerstate.context = { ...contextArg };
    innerstate.state = null;
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

  const selfNextCanonize = () =>
    (codegraph.nextMap.at(data.legIndex) || [])
      .filter((x): x is CodeGraph.NextCCanonize => x.code.t === "canonize")
      .at(0);

  const selfPendingCanonizationAdvrt =
    (): PendingCanonizationAdvert<CType> | null => {
      const nextCanonize = selfNextCanonize();

      if (nextCanonize) {
        const lastEvent = getSelfStateOrThrow(
          "fatal error: state is null where next has canonize"
        );
        const {
          meta: { eventId: timelineOf },
          payload: { t: stateName },
        } = lastEvent;
        const depth = multiverse.depthOf(lastEvent.meta.eventId);

        const existingAdvrt = swarmStore.canonizationStore
          .getAdvertisementsForAddress(stateName, depth)
          .find((req) => req.payload.timelineOf === timelineOf);

        if (!existingAdvrt) {
          const uniqueActorDesignation = nextCanonize.code.actor.get();
          const canonizer = innerstate.context[uniqueActorDesignation];

          return { canonizer, name: stateName, depth, timelineOf };
        }
      }

      return null;
    };

  const isWaitingForCanonization = () => {
    const next = (codegraph.nextMap.at(data.legIndex) || [])
      .filter((x): x is CodeGraph.NextCCanonize => x.code.t === "canonize")
      .at(0);

    if (!next) return false;

    const atStack = data.stack.at(next.index);
    if (atStack?.t !== "canonize") {
      throw new Error(
        "fatal error canonize is not populated by AssignAhead mechanism"
      );
    }
    // check for existing forTimeline
    const name = atStack.lastEvent.payload.t;
    const depth = multiverse.depthOf(atStack.lastEvent.meta.eventId);

    if (atStack?.t !== "canonize") return false;

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

      const match = getSMatchAtIndex(data.legIndex);
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
      const res = selfAvailableCompensateable(data.legIndex).map(
        ({ codeIndex, fromTimelineOf, involvedActors }) => ({
          codeIndex,
          fromTimelineOf,
          involvedActors,
        })
      );

      const match = getSMatchAtIndex(data.legIndex);
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
        ({ codeIndex, fromTimelineOf, involvedActors }) => ({
          codeIndex,
          fromTimelineOf,
          involvedActors,
        })
      );

      const match = getSMatchAtIndex(data.legIndex);
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
      const res = selfDoneCompensation(data.legIndex);
      const match = getSMatchAtIndex(data.legIndex);
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
      getSMatchAtIndex(data.legIndex)?.inner.pendingCanonizationAdvrt() ||
      null,
    advanceToMostCanon,
    resetAndAdvanceToMostCanon: () => {
      reset();
      advanceToMostCanon();
    },
    resetAndAdvanceToEventId,
    isInvolved: (paramId) => {
      const actorSet = codegraph.actorSetMap.at(data.legIndex);
      if (!actorSet) return false;

      const foundIndex = actorSet.toArray().findIndex((involvement) => {
        if (
          involvement.t === "Unique" &&
          innerstate.context[involvement.get()] === paramId.id
        ) {
          return true;
        }

        if (involvement.t === "Role" && involvement.get() === paramId.role) {
          return true;
        }

        return false;
      });

      return foundIndex !== -1;
    },
    codegraph: () => codegraph,
    codegraphBranchesFromCompensation: () => {
      const match = getSMatchAtIndex(data.legIndex);
      if (match?.inner) return match.inner.codegraphBranchesFromCompensation();

      const activeCompensation = selfActiveCompensation().at(0);
      if (activeCompensation) {
        const branches = codegraph.branchesFrom(data.legIndex);
        return branches.map((branch) =>
          convertBranchOfToNamed(innerstate.context, branch)
        );
      }

      // closest availableCompensateable
      const availableCompensateable = selfAvailableCompensateable()
        .sort((a, b) =>
          Ord.toNum(NestedCodeIndexAddress.cmp(b.codeIndex, a.codeIndex))
        )
        .at(0);
      if (availableCompensateable) {
        const bounding = ccompensateIndexer.withList.find(
          (x) => x.start === availableCompensateable.withIndex
        );
        if (bounding) {
          // find branches inside the compensate-with scope
          const branches = codegraph
            .branchesFrom(data.legIndex)
            .filter((line) => {
              const lastNode = line.chain.at(line.chain.length - 1);
              if (!lastNode) return false;
              return lastNode > bounding.start && lastNode <= bounding.end;
            });
          return branches.map((branch) =>
            convertBranchOfToNamed(innerstate.context, branch)
          );
        }
      }

      return null;
    },
    logger,
  };

  return self;
};
