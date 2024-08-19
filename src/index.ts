import {
  CancelSubscription,
  EventsMsg,
  EventsOrTimetravel,
  MsgType,
  OnCompleteOrErr,
  Tag,
  TaggedEvent,
  TaggedTypedEvent,
  Tags,
} from "@actyx/sdk";
import * as Reality from "./multiverse.js";
import { SwarmData, WFMachine } from "./wfmachine.js";
export { Reality };
import { Node, XEventKey } from "./ax-mock/index.js";
import {
  divergencePoint,
  CTypeProto,
  InternalTag,
  WFBusinessOrMarker,
  extractWFCompensationMarker,
  extractWFBusinessEvents,
  WFCompensationMarker,
  WFMarkerCompensationNeeded,
  WFMarkerCompensationDone,
  NestedCodeIndexAddress,
  extractWFCanonMarker,
  ActyxWFCanonAdvrt,
  WFMarkerCanonAdvrt,
  WFMarkerCanonDecide,
  extractWFCanonDecideMarker,
  sortByEventKey,
} from "./consts.js";
import { WFWorkflow } from "./wfcode.js";
import { createLinearChain } from "./event-utils.js";
import { Logger, makeLogger, MultihashMap, Ord } from "./utils.js";

export type Params<CType extends CTypeProto> = {
  // actyx: Parameters<(typeof Actyx)["of"]>;
  tags: Tags<WFBusinessOrMarker<CType>>;
  self: {
    role: CType["role"];
    id: string;
  };
};

type OnEventsOrTimetravel<E> = (data: EventsOrTimetravel<E>) => Promise<void>;

/**
 * Is a union of state of a MachineCombinator. A MachineCombinator can have
 * multiple WFMachines to make compensation calculations.
 */
type DataModes<CType extends CTypeProto> = {
  wfMachine: WFMachine<CType>;
} & (
  | { t: "normal" }
  | { t: "catching-up" }
  | {
      t: "off-canon";
      canonWFMachine: WFMachine<CType>;
      compensationInfo: WFMarkerCompensationNeeded;
    }
);

export type Machine<CType extends CTypeProto> = ReturnType<typeof run<CType>>;

// TODO:
// - involve participation into compensation calculation (this depends on
//   whether we want to block everyone involved in the task into until a
//   particular compensation is done by a subset of participant)

/**
 * Creates a machine combinator and listens to an actyx node.
 */
export const run = <CType extends CTypeProto>(
  params: Params<CType>,
  node: Node.Type<WFBusinessOrMarker<CType>>,
  workflow: WFWorkflow<CType>
) => {
  const machineCombinator = MachineCombinator.make(params, workflow);
  const { canonizationBarrier, multiverseTree } = machineCombinator;

  const batchedPublishes = () => {
    const batch: TaggedEvent[] = [];
    const collect: CollectEvents<CType> = (x) => batch.push(x);
    const publish = () => {
      const res =
        batch.length > 0
          ? node.api.publishMany(batch.slice())
          : Promise.resolve();
      batch.length = 0;
      return res;
    };

    return { collect, publish };
  };

  const axSub = perpetualSubscription(node, async (e) => {
    if (e.type === MsgType.timetravel) {
      machineCombinator.setToCatchingUp();
    } else if (e.type === MsgType.events) {
      // Publishing should happen after MachineCombinator has finished all its
      // internal transformations.
      const batch = batchedPublishes();
      // `pipe` function when run will internally issue publishes for
      // compensation and canonization tracking. These publish issuances will be
      // batched and executed after `pipe` is finished.
      machineCombinator.pipe(e, batch.collect);
      // TODO: think more about the potentially async stuff here on the real
      // implementation
      batch.publish();
    }
  });

  /**
   * This function body is the definition of commands available to this machine
   */
  const commands = () => {
    const data = machineCombinator.internal();

    // We don't do anything while catching up. TODO: There might be a lot of
    // "catching-up" if a lot of partition join happens, should think about this
    // for at large-scale swarm.
    if (data.t === "catching-up") return [];

    const commands = data.wfMachine
      .availableNexts()
      .filter((x) => {
        if (x.actor.t === "Unique") {
          return x.actor.get() === params.self.id;
        } else {
          return x.actor.get() === params.self.role;
        }
      })
      .map((info) => ({
        info,
        publish: (payloadPatch: Record<string, unknown> = {}) => {
          const payload = { ...payloadPatch };
          let tags = params.tags;

          const last = machineCombinator.last();
          if (last) {
            tags = tags.and(
              Tag<WFBusinessOrMarker<CType>>(
                InternalTag.Predecessor.write(last.meta.eventId)
              )
            );
          }

          const compensationInfo = machineCombinator.compensation();
          if (compensationInfo) {
            tags = tags.and(
              Tag<WFBusinessOrMarker<CType>>(
                InternalTag.CompensationEvent.write(
                  compensationInfo.fromTimelineOf
                )
              )
            );
          }

          tags = tags.and(
            Tag<WFBusinessOrMarker<CType>>(
              InternalTag.ActorWriter.write(params.self.id)
            )
          );

          node.api.publish(tags.applyTyped({ t: info.name, payload }));
        },
      }));

    if (data.t === "off-canon") {
      // TODO: examine whether the entire swarm needs to be paused until a
      // canonization is decided, or just partially
      const compensateableIsInvolvedInRequestedCanonization =
        canonizationBarrier.active.involvedEventIds.has(
          data.compensationInfo.fromTimelineOf
        );

      if (compensateableIsInvolvedInRequestedCanonization) {
        return [];
      }

      return commands.filter((x) => x.info.reason.has("compensation"));
    }

    return commands;
  };

  return {
    commands,
    compensation: machineCombinator.compensation,
    mcomb: () => machineCombinator.internal(),
    multiverseTree: () => multiverseTree,
    wfmachine: () => machineCombinator.wfmachine(),
    state: () => machineCombinator.wfmachine().state(),
    logger: machineCombinator.logger,
    kill: axSub.kill,
  };
};

/**
 * Subscribe, always reconnect on connection error.
 */
const perpetualSubscription = <E>(
  node: Node.Type<E>,
  onEventsOrTimeTravel: OnEventsOrTimetravel<E>
) => {
  let alive = true;
  let killSub: CancelSubscription | null = null;

  const onCompleteOrErr: OnCompleteOrErr = (err) => {
    if (err) console.error(err);
    if (!alive) return;
    connect();
  };

  const connect = () => {
    if (killSub === null) {
      killSub = node.api.subscribeMonotonic(
        onEventsOrTimeTravel,
        onCompleteOrErr
      );
    }
  };

  connect();

  return {
    kill: () => {
      alive = false;
      killSub?.();
    },
  };
};

type CollectEvents<CType extends CTypeProto> = (
  e: TaggedTypedEvent<WFBusinessOrMarker<CType>>
) => unknown;

export namespace MachineCombinator {
  /**
   * Creates a MachineCombinator.
   *
   * MachineCombinator manages a multiverse tree and uses several WFMachines to
   * detect compensations.
   */
  export const make = <CType extends CTypeProto>(
    params: Params<CType>,
    workflow: WFWorkflow<CType>
  ) => {
    const logger = makeLogger(`mcomb:${params.self.id}`);
    const multiverseTree = Reality.MultiverseTree.make<CType>();
    const canonizationStore = Reality.CanonizationStore.make<CType>();
    canonizationStore.logger.sub(logger.log);
    const swarmStore: SwarmData<CType> = {
      multiverseTree,
      canonizationStore: canonizationStore,
    };
    const compensationMap = CompensationMap.make();
    const wfMachine = WFMachine(workflow, swarmStore);
    wfMachine.logger.sub(logger.log);

    const canonizationBarrier: CanonizationBarrier<CType> = {
      active: {
        activeAdvertisements: [],
        involvedEventIds: new Set(),
      },
    };

    const refreshCanonizationbarrier = () => {
      canonizationBarrier.active = generateCanonizationBarrier(
        multiverseTree,
        canonizationStore
      );
    };

    refreshCanonizationbarrier();

    const internal = new Proxy<{ data: DataModes<CType> }>(
      {
        data: {
          t: "catching-up",
          wfMachine,
        },
      },
      {
        /* proxy for debugging purpose */
      }
    );

    const currentCompensation = () =>
      internal.data.t === "off-canon" ? internal.data.compensationInfo : null;

    /**
     * Recalculate supposed actual DataModes for this MachineCombinator.
     */
    // TODO: should compensation happen in timeouts/failures/retries?
    const recalc = (publish: CollectEvents<CType>) => {
      // predecessorMap.getBackwardChain(compensationMap.getByActor(...))
      // TODO: return null means something abnormal happens in predecessorMap e.g. missing root, missing event details
      // TODO: think about compensation events tag
      const wfMachine = internal.data.wfMachine;
      wfMachine.advanceToMostCanon();

      const canonWFMachine = WFMachine(workflow, swarmStore);
      canonWFMachine.logger.sub(logger.log);
      canonWFMachine.advanceToMostCanon();

      const compensateables = calculateCompensateables(
        params.self,
        workflow,
        swarmStore,
        wfMachine,
        canonWFMachine,
        logger
      );

      // compensateables are registered whenever a wfmachine is off-canon
      // remember: this compensateables are only list of compensateables that applies to self, not other actor in the swarm.
      compensateables.forEach(({ fromTimelineOf, toTimelineOf, codeIndex }) => {
        const directive: WFMarkerCompensationNeeded = {
          ax: InternalTag.CompensationNeeded.write(""),
          actor: params.self.id,
          fromTimelineOf,
          toTimelineOf,
          codeIndex,
        };

        if (!compensationMap.hasRegistered(directive)) {
          compensationMap.register(directive);
          publish(params.tags.applyTyped(directive));
        }
      });

      // Now work with remembered compensatebles. We naturally work with the highest codeIndex (the deepest nesting)
      const allRememberedComps = compensationMap
        .getByActor(params.self.id)
        .sort((a, b) =>
          Ord.toNum(
            NestedCodeIndexAddress.cmp(
              b.directive.codeIndex,
              a.directive.codeIndex
            )
          )
        );

      // compensateable implies the compensation is not yet active; it can be marked as done when canonization happens in the actor's favor
      // active compensation, however is more complicated. If it is already happening, it must be followed through by the actors until it is done.
      // resolve dangling compensateables
      const remainingRememberedComps = allRememberedComps
        .map((comp) => {
          // remembered compensateable can be invalidated if:
          // - it is back to canon and it is not active, or
          // - the compensation is done
          const compMachine = WFMachine(workflow, swarmStore); // can be optimized further by checking wfmachine's history
          compMachine.logger.sub(logger.log);
          compMachine.resetAndAdvanceToEventId(comp.fromTimelineOf);
          compMachine.advanceToMostCanon();

          // check if compensation is not active and compMachine is canon
          const compMachineIsCanon =
            compMachine.latestStateEvent()?.meta.eventId ===
            canonWFMachine.latestStateEvent()?.meta.eventId;

          if (compMachineIsCanon) {
            const isActive =
              compMachine
                .activeCompensation()
                .find(
                  (activeComp) =>
                    activeComp.fromTimelineOf === comp.toTimelineOf
                ) !== undefined;

            if (!isActive) {
              const directive: WFMarkerCompensationDone = {
                ax: InternalTag.CompensationDone.write(""),
                actor: params.self.id,
                fromTimelineOf: comp.fromTimelineOf,
                toTimelineOf: comp.toTimelineOf,
              };
              if (!compensationMap.hasRegistered(directive)) {
                compensationMap.register(directive);
                publish(params.tags.applyTyped(directive));
              }

              return null;
            }
          }

          const isDone =
            compMachine
              .doneCompensation()
              .find(
                (x) =>
                  NestedCodeIndexAddress.cmp(
                    x.codeIndex,
                    comp.directive.codeIndex
                  ) === Ord.Equal && x.fromTimelineOf === comp.fromTimelineOf
              ) !== undefined;

          if (isDone) {
            const directive: WFMarkerCompensationDone = {
              ax: InternalTag.CompensationDone.write(""),
              actor: params.self.id,
              fromTimelineOf: comp.fromTimelineOf,
              toTimelineOf: comp.toTimelineOf,
            };
            if (!compensationMap.hasRegistered(directive)) {
              compensationMap.register(directive);
              publish(params.tags.applyTyped(directive));
            }

            return null;
          }

          return {
            compensateable: comp,
            compMachine: compMachine,
          };
        })
        .filter((x): x is NonNullable<typeof x> => x !== null);

      const firstRememberedComps = remainingRememberedComps.at(0);

      if (firstRememberedComps) {
        internal.data = {
          t: "off-canon",
          wfMachine: firstRememberedComps.compMachine,
          canonWFMachine: canonWFMachine,
          compensationInfo: firstRememberedComps.compensateable.directive,
        };
      } else {
        internal.data = {
          t: "normal",
          wfMachine: canonWFMachine,
        };
      }
    };

    const emitPendingCanonizationAdvertisements = (
      publish: CollectEvents<CType>
    ) => {
      const pendingCanonizationAdvrt =
        internal.data.wfMachine.pendingCanonizationAdvrt();
      if (!pendingCanonizationAdvrt) return;

      const lastStateIsOwn =
        internal.data.wfMachine
          .state()
          .state?.[1].meta.tags.includes(
            InternalTag.ActorWriter.write(params.self.id)
          ) || false;

      if (!lastStateIsOwn) return;

      publish(
        params.tags.applyTyped({
          ...pendingCanonizationAdvrt,
          advertiser: params.self.id,
          ax: InternalTag.CanonAdvrt.write(""),
        } satisfies WFMarkerCanonAdvrt<CType>)
      );
    };

    // TODO: how to automate this? which one to take? strategy?
    const canonizeWhenPossible = (publish: CollectEvents<CType>) => {
      const canonizables =
        canonizationBarrier.active.activeAdvertisements.filter(
          (ad) => ad.advrt.payload.canonizer === params.self.id
        );

      if (canonizables.length === 0) return;

      const groupedByNameAndDepth = MultihashMap.depth(2).make<
        [CType["ev"], number],
        typeof canonizables
      >();

      canonizables.forEach((canonizable) => {
        const nameDepthKey: MultihashMap.KeyOf<typeof groupedByNameAndDepth> = [
          canonizable.advrt.payload.name,
          canonizable.advrt.payload.depth,
        ];
        // populate groupedBynameAndDepth
        const nameDepthGroup = groupedByNameAndDepth.get(nameDepthKey) || [];
        if (!groupedByNameAndDepth.has(nameDepthKey))
          groupedByNameAndDepth.set(nameDepthKey, nameDepthGroup);
        nameDepthGroup.push(canonizable);
      });

      Array.from(groupedByNameAndDepth.values()).forEach((advrts) => {
        // TODO: introduce multiple strategies.

        // The default: check the timelines for the business event pointed by
        // the advertisements. Choose one based on the sort key of the business event.
        const advrtFromBusiness = advrts
          .map(({ advrt, history }) => {
            const businessEvent = multiverseTree.getById(
              advrt.payload.timelineOf
            );
            if (!businessEvent) return;

            return {
              advrt,
              businessEvent,
              eventKey: XEventKey.fromMeta(businessEvent.meta),
            };
          })
          .filter((x): x is NonNullable<typeof x> => x !== null)
          .sort((a, b) => Ord.toNum(Ord.cmp(a.eventKey, b.eventKey)))
          .map((x) => x.advrt)
          .at(0);

        // in case the business event is not found (syntactically possible,
        // logically not);
        const advrtForBackup = sortByEventKey(advrts.map((x) => x.advrt)).at(0);

        const chosenAdvrt = advrtFromBusiness || advrtForBackup;

        if (!chosenAdvrt) return;

        const canonization: WFMarkerCanonDecide<CType> = {
          ax: InternalTag.CanonDecide.write(""),
          name: chosenAdvrt.payload.name,
          depth: chosenAdvrt.payload.depth,
          timelineOf: chosenAdvrt.payload.timelineOf,
          canonizer: params.self.id,
        };

        publish(params.tags.applyTyped(canonization));
      });
    };

    return {
      logger,
      multiverseTree,
      canonizationBarrier,
      canonizationStore,
      recalc,
      internal: () => internal.data,
      wfmachine: () => internal.data.wfMachine,
      compensation: currentCompensation,
      last: () => internal.data.wfMachine.latestStateEvent(),
      setToCatchingUp: () => {
        internal.data = {
          t: "catching-up",
          wfMachine: internal.data.wfMachine,
        };
      },
      pipe: (
        e: EventsMsg<WFBusinessOrMarker<CType>>,
        publish: CollectEvents<CType>
      ) => {
        // categorize events
        const wfBusinessEvents = extractWFBusinessEvents(e.events);
        const compsMarker = extractWFCompensationMarker(e.events);
        const canonMarkers = extractWFCanonMarker(e.events);
        const canonDecisionMarkers = extractWFCanonDecideMarker(canonMarkers);

        // apply to storage
        wfBusinessEvents.forEach(multiverseTree.register);
        compsMarker.map((ev) => compensationMap.register(ev.payload));
        canonMarkers.forEach(swarmStore.canonizationStore.register);

        if (canonMarkers.length > 0) {
          refreshCanonizationbarrier();
          canonizeWhenPossible(publish);
        }

        // If a canon decision is triggered, mark as catching-up
        const needRecalc =
          canonDecisionMarkers.length > 0 || compsMarker.length > 0;

        const data = internal.data;

        if (data.t === "catching-up" && e.caughtUp) {
          recalc(publish);
        } else if (data.t === "normal") {
          data.wfMachine.advanceToMostCanon();

          if (data.wfMachine.doneCompensation().length > 0 || needRecalc) {
            recalc(publish);
          }
        } else if (data.t === "off-canon") {
          data.wfMachine.advanceToMostCanon();
          data.canonWFMachine.advanceToMostCanon();

          if (data.wfMachine.doneCompensation().length > 0 || needRecalc) {
            recalc(publish);
          }
        }

        emitPendingCanonizationAdvertisements(publish);
      },
    };
  };
}

// TODO: fix tracking, track compensation indicies instead
export namespace CompensationMap {
  type Actor = string;
  type From = string;
  type To = string;

  /**
   * A map that keeps track of actors' (that's being referred to by the
   * identity) need to compensate (or observe a compensation sequence).
   */
  export const make = () => {
    const data = {
      positive: new Map<
        Actor,
        Map<From, Map<To, WFMarkerCompensationNeeded>>
      >(),
      negative: new Map<Actor, Map<From, Map<To, boolean>>>(),
    };

    const access = <T>(
      entry: Map<Actor, Map<From, Map<To, T>>>,
      { actor, fromTimelineOf: from }: WFCompensationMarker
    ) => {
      const fromMap: Exclude<
        ReturnType<(typeof entry)["get"]>,
        undefined
      > = entry.get(actor) || new Map();
      entry.set(actor, fromMap);

      const toMap: Exclude<
        ReturnType<(typeof fromMap)["get"]>,
        undefined
      > = fromMap.get(from) || new Map();
      fromMap.set(from, toMap);

      return toMap;
    };

    return {
      hasRegistered: (compensation: WFCompensationMarker): boolean => {
        if (InternalTag.CompensationNeeded.is(compensation.ax)) {
          const needed = compensation as WFMarkerCompensationNeeded;
          const set = access(data.positive, compensation);
          const entry = set.get(needed.toTimelineOf);
          return (
            entry !== undefined &&
            NestedCodeIndexAddress.cmp(entry.codeIndex, needed.codeIndex) ===
              Ord.Equal
          );
        } else if (InternalTag.CompensationDone.is(compensation.ax)) {
          const done = compensation as WFMarkerCompensationDone;
          const set = access(data.negative, compensation);
          return set.has(done.toTimelineOf);
        }
        return false;
      },
      register: (compensation: WFCompensationMarker) => {
        // TODO: runtime validation
        if (InternalTag.CompensationNeeded.is(compensation.ax)) {
          const needed = compensation as WFMarkerCompensationNeeded;
          const set = access(data.positive, compensation);
          set.set(needed.toTimelineOf, needed);
        } else if (InternalTag.CompensationDone.is(compensation.ax)) {
          const done = compensation as WFMarkerCompensationDone;
          const set = access(data.negative, compensation);
          set.set(done.toTimelineOf, true);
        }
      },
      getByActor: (actor: string) => {
        const ret = [] as {
          fromTimelineOf: string;
          toTimelineOf: string;
          directive: WFMarkerCompensationNeeded;
        }[];
        const fromMap = data.positive.get(actor);
        if (!fromMap) return [];

        Array.from(fromMap.entries()).forEach(([fromEventId, toSet]) => {
          Array.from(toSet).forEach(([toEventId, directive]) => {
            const hasNegative =
              data.negative.get(actor)?.get(fromEventId)?.has(toEventId) ||
              false;

            if (!hasNegative) {
              ret.push({
                fromTimelineOf: fromEventId,
                toTimelineOf: toEventId,
                directive,
              });
            }
          });
        });

        return ret;
      },
    };
  };
}

type CanonizationBarrier<CType extends CTypeProto> = {
  active: {
    readonly activeAdvertisements: {
      advrt: ActyxWFCanonAdvrt<CType>;
      history: string[];
    }[];
    /**
     * List of state's eventId machines SHOULD NOT jump out of due to active canonization ad
     */
    readonly involvedEventIds: Set<string>;
  };
};

// TODO: should be part of CanonizationStore eventually
const generateCanonizationBarrier = <CType extends CTypeProto>(
  multiverse: Reality.MultiverseTree.Type<CType>,
  canonMap: Reality.CanonizationStore.Type<CType>
): CanonizationBarrier<CType>["active"] => {
  const activeAdvertisements = canonMap.getOpenAdvertisements().map((advrt) => {
    const history = (() => {
      const point = multiverse.getById(advrt.payload.timelineOf);
      if (!point) return [];
      return createLinearChain(multiverse, point).map((x) => x.meta.eventId);
    })();

    return {
      advrt,
      history,
    };
  });
  const involvedEventIds = new Set(
    activeAdvertisements.flatMap((x) => x.history)
  );

  return { activeAdvertisements, involvedEventIds };
};

/**
 * Calculate the compensation needed when a machine jumps from a point in the
 * multiverse to the other.
 */
const calculateCompensateables = <CType extends CTypeProto>(
  self: {
    role: CType["role"];
    id: string;
  },
  workflow: WFWorkflow<CType>,
  swarmData: SwarmData<CType>,
  fromWFMachine: WFMachine<CType>,
  toWFMachine: WFMachine<CType>,
  logger: Logger
) => {
  // TODO: Assuming `multiverse tree` is secure from malicious actors and is correctly implemented,
  // faster calculation can be done via workflow code analysis.
  const { multiverseTree } = swarmData;

  const fromPoint = fromWFMachine.latestStateEvent();
  const toPoint = toWFMachine.latestStateEvent();

  // compensateable to an empty chain is impossible
  if (!toPoint) return [];

  const fromChain =
    (fromPoint && createLinearChain(multiverseTree, fromPoint)) || [];
  const toChain = (toPoint && createLinearChain(multiverseTree, toPoint)) || [];
  const divergence = divergencePoint(fromChain, toChain);

  // fromChain is sub-array of toChain
  if (divergence === fromChain.length - 1) return [];

  const divergenceWFmachine = WFMachine(workflow, swarmData);
  divergenceWFmachine.logger.sub(logger.log);

  // -1 means not found, similar to .findIndex array returns
  if (divergence > -1) {
    const atDivergence = fromChain[divergence];
    divergenceWFmachine.resetAndAdvanceToEventId(atDivergence.meta.eventId);
  }
  // Comps before divergence should not be accounted for
  const compsBeforeDivergence = divergenceWFmachine
    .availableCompensateable()
    .filter((comp) => comp.involvedActors.includes(self.id));
  const allActiveCompensations = fromWFMachine
    .availableCompensateable()
    .filter((comp) => comp.involvedActors.includes(self.id));

  // subtract "before-divergence" from "all" and we get compensations that we need
  // TODO: might want to check for more than `codeIndex` because this might not be completely the right definition.
  const activeCompensationsBetweenFromAndTwo = Array.from(
    allActiveCompensations
  ).filter((x) => {
    const existsBeforeDivergence = compsBeforeDivergence.find(
      (y) =>
        NestedCodeIndexAddress.cmp(x.codeIndex, y.codeIndex) === Ord.Equal &&
        x.fromTimelineOf === y.fromTimelineOf
    );

    // Note: compensateable = comps that exists only after the divergence points, i.e. it doesn't exist before divergence.
    return !existsBeforeDivergence;
  });

  return activeCompensationsBetweenFromAndTwo.map((x) => ({
    /**
     * The "from" attribute is identified by the first event within the compensation block, not from the "from point".
     */
    fromTimelineOf: x.fromTimelineOf,
    toTimelineOf: toPoint.meta.eventId,
    codeIndex: x.codeIndex,
  }));
};
