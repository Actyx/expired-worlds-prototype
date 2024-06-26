import {
  CancelSubscription,
  EventsMsg,
  EventsOrTimetravel,
  MsgType,
  OnCompleteOrErr,
  Tag,
  Tags,
} from "@actyx/sdk";
import * as Reality from "./reality.js";
import { WFMachine } from "./wfmachine.js";
export { Reality };
import { Node } from "./ax-mock/index.js";
import {
  divergencePoint,
  CTypeProto,
  InternalTag,
  WFBusinessOrMarker,
  extractWFDirective,
  extractWFEvents,
  WFMarker,
  WFMarkerCompensationNeeded,
  WFMarkerCompensationDone,
  ActyxWFBusiness,
} from "./consts.js";
import { WFWorkflow } from "./wfcode.js";
import { createLinearChain } from "./event-utils.js";

export type Params<CType extends CTypeProto> = {
  // actyx: Parameters<(typeof Actyx)["of"]>;
  tags: Tags<WFBusinessOrMarker<CType>>;
  self: {
    role: CType["role"];
    id: string;
  };
};

type OnEventsOrTimetravel<E> = (data: EventsOrTimetravel<E>) => Promise<void>;
type DataModes<CType extends CTypeProto> = {
  wfMachine: WFMachine<CType>;
} & (
  | { t: "normal" }
  | { t: "building-compensations" }
  | {
      t: "compensating";
      canonWFMachine: WFMachine<CType>;
      compensationInfo: WFMarkerCompensationNeeded;
    }
);

// TODO:
// - involve participation into compensation calculation (this depends on whether we want to block everyone involved in the task into until a particular compensation is done by a subset of participant)
export const run = <CType extends CTypeProto>(
  params: Params<CType>,
  // const node = Node.make<WFEventAndDirective<CType>>({ id: params.id });
  node: Node.Type<WFBusinessOrMarker<CType>>,
  workflow: WFWorkflow<CType>
) => {
  const machineCombinator = MachineCombinator.make(params, node, workflow);
  const axSub = perpetualSubscription(node, async (e) => {
    if (e.type === MsgType.timetravel) {
      machineCombinator.setToBuildingCompensation();
    } else if (e.type === MsgType.events) {
      machineCombinator.pipe(e);
    }
  });

  const commands = () => {
    const data = machineCombinator.internal();
    if (data.t === "building-compensations") {
      return [];
    }

    const commands = data.wfMachine
      .availableNexts()
      .filter((x) => {
        if (x.actor.t === "Role") {
          return x.actor.get() === params.self.role;
        } else {
          return x.actor.get() === params.self.id;
        }
      })
      .map((x) => ({
        info: x,
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

          node.api.publish(tags.applyTyped({ t: x.name, payload }));
        },
      }));

    if (data.t === "compensating") {
      return commands.filter((x) => x.info.reason === "compensation");
    }

    return commands;
  };

  return {
    commands,
    mcomb: () => machineCombinator.internal(),
    machine: () => machineCombinator.machine(),
    state: () => machineCombinator.machine().state(),
    kill: axSub.kill,
  };
};

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

export namespace MachineCombinator {
  export const make = <CType extends CTypeProto>(
    params: Params<CType>,
    node: Node.Type<WFBusinessOrMarker<CType>>,
    workflow: WFWorkflow<CType>
  ) => {
    const multiverseTree = Reality.MultiverseTree.make<CType>();
    const compensationMap = CompensationMap.make();
    let data: DataModes<CType> = {
      t: "building-compensations",
      wfMachine: WFMachine(workflow, multiverseTree),
    };

    const currentCompensation = () =>
      data.t === "compensating" ? data.compensationInfo : null;

    /**
     * There can be discrepancies between the compensations noted in the
     * CompensationMap and WFMachine because an actor can stop working at the moment
     * between 1.) when the compensation is finished and 2.) when the
     * "CompensationDone" marker is published
     *
     * Note: Does this really work to invalidate compensations?
     */
    const compareRememberedCompensation = (
      rememberedCompensation: ReturnType<
        (typeof compensationMap)["getByActor"]
      >[any]
    ) => {
      const fromlastEvent = multiverseTree.getById(
        rememberedCompensation.fromTimelineOf
      );
      const toLastEvent = multiverseTree.getById(
        rememberedCompensation.toTimelineOf
      );
      // TODO: handle more gracefully
      if (!fromlastEvent || !toLastEvent) {
        throw new Error("missing to or from event noted in a compensation");
      }

      // Do a comparison between remembered compensations and the actually needed compensations
      const actualCompensations = calculateCompensations(
        workflow,
        multiverseTree,
        fromlastEvent,
        toLastEvent
      );

      const matchingCompensation = (() => {
        if (!actualCompensations) return null;
        const matchingCompensation = actualCompensations.compensations.find(
          (x) =>
            x.fromTimelineOf === rememberedCompensation.fromTimelineOf &&
            x.toTimelineOf === rememberedCompensation.toTimelineOf &&
            x.codeIndex === rememberedCompensation.directive.codeIndex
        );
        if (!matchingCompensation) return null;

        return {
          compensation: matchingCompensation,
          lastMachineState: actualCompensations.lastMachineState,
        } as const;
      })();

      if (!matchingCompensation) return null;

      return {
        matchingCompensation,
      };
    };

    const recalc = () => {
      // predecessorMap.getBackwardChain(compensationMap.getByActor(...))
      // TODO: return null means something abnormal happens in predecessorMap e.g. missing root, missing event details
      // TODO: think about compensation events tag
      const canonWFMachine = WFMachine(workflow, multiverseTree);
      canonWFMachine.resetAndAdvanceToMostCanon();

      const rememberedCompensation = compensationMap
        .getByActor(params.self.id)
        .sort((a, b) => b.directive.codeIndex - a.directive.codeIndex)
        .at(0);

      if (rememberedCompensation) {
        const compensationComparison = compareRememberedCompensation(
          rememberedCompensation
        );

        if (compensationComparison) {
          data = {
            t: "compensating",
            wfMachine:
              compensationComparison.matchingCompensation.lastMachineState,
            canonWFMachine: canonWFMachine,
            compensationInfo: rememberedCompensation.directive,
          };
        } else {
          // Indication that the compensation is actually done but unmarked:
          // When WFMachine says that the compensation is done but the compensationMap remembers differently
          // Mark the compensation as done
          const directive: WFMarkerCompensationDone = {
            ax: InternalTag.CompensationDone.write(""),
            actor: params.self.id,
            fromTimelineOf: rememberedCompensation.fromTimelineOf,
            toTimelineOf: rememberedCompensation.toTimelineOf,
          };
          // TODO: fix tag
          node.api.publish(params.tags.apply(directive));

          data = {
            t: "normal",
            wfMachine: canonWFMachine,
          };
        }
      } else {
        data = {
          t: "normal",
          wfMachine: canonWFMachine,
        };
      }
    };

    return {
      recalc,
      internal: () => data,
      machine: () => data.wfMachine,
      compensation: currentCompensation,
      last: () => data.wfMachine.latestStateEvent(),
      setToBuildingCompensation: () => {
        data = {
          t: "building-compensations",
          wfMachine: data.wfMachine,
        };
      },
      pipe: (e: EventsMsg<WFBusinessOrMarker<CType>>) => {
        extractWFEvents(e.events).map(multiverseTree.register);
        extractWFDirective(e.events).map((ev) => {
          compensationMap.register(ev.payload);
        });

        const currentData = data;
        if (currentData.t === "building-compensations" && e.caughtUp) {
          const canonWFMachine = WFMachine(workflow, multiverseTree);
          canonWFMachine.advanceToMostCanon();

          const lastEvent = currentData.wfMachine.latestStateEvent();
          const canonLastEvent = canonWFMachine.latestStateEvent();

          const compensations =
            lastEvent &&
            canonLastEvent &&
            calculateCompensations(
              workflow,
              multiverseTree,
              lastEvent,
              canonLastEvent
            )?.compensations;
          if (compensations) {
            // register compensations to both compensation map and the persistence
            // layer: ax
            compensations.forEach(
              ({ fromTimelineOf, toTimelineOf, codeIndex }) => {
                const directive: WFMarker = {
                  ax: InternalTag.CompensationNeeded.write(""),
                  actor: params.self.id,
                  fromTimelineOf,
                  toTimelineOf,
                  codeIndex,
                };
                // TODO: fix tag
                node.api.publish(params.tags.apply(directive));
                compensationMap.register(directive);
              }
            );
          }
          recalc();

          return;
        }

        if (currentData.t === "normal") {
          currentData.wfMachine.advanceToMostCanon();
          return;
        }

        if (currentData.t === "compensating") {
          currentData.wfMachine.advanceToMostCanon();
          currentData.canonWFMachine.advanceToMostCanon();

          // check if compensation still applies
          // TODO: optimize compensation query
          const activeCompensationCode = currentData.wfMachine
            .availableCompensateable()
            .find(
              (x) =>
                x.fromTimelineOf ===
                  currentData.compensationInfo.fromTimelineOf &&
                x.codeIndex === currentData.compensationInfo.codeIndex
            );

          // compensation is done
          if (!activeCompensationCode) {
            recalc();
          }
        }
      },
    };
  };
}

// TODO: fix tracking, track compensation indicies instead
export namespace CompensationMap {
  type Actor = string;
  type From = string;
  type To = string;

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
      { actor, fromTimelineOf: from }: WFMarker
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
      register: (compensation: WFMarker) => {
        // TODO: runtime validation
        if (InternalTag.CompensationNeeded.is(compensation.ax)) {
          const needed = compensation as WFMarkerCompensationNeeded;
          const set = access(data.positive, compensation);
          set.set(needed.toTimelineOf, needed);
        } else if (InternalTag.CompensationDone.is(compensation.ax)) {
          const done = compensation as WFMarkerCompensationDone;
          const set = access(data.negative, compensation);
          set.delete(done.toTimelineOf);
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

const calculateCompensations = <CType extends CTypeProto>(
  workflow: WFWorkflow<CType>,
  multiverse: Reality.MultiverseTree.Type<CType>,
  fromPoint: ActyxWFBusiness<CType>,
  toPoint: ActyxWFBusiness<CType>
) => {
  // TODO: this is wrong. to do calculate compensations, one must also calculate
  const fromChain = createLinearChain(multiverse, fromPoint);
  const toChain = createLinearChain(multiverse, toPoint);
  const divergence = divergencePoint(fromChain, toChain);

  if (divergence === fromChain.length - 1) return null;

  const simulation = WFMachine(workflow, multiverse);

  // -1 means not found, similar to .findIndex array returns
  if (divergence > -1) {
    const atDivergence = fromChain[divergence];
    simulation.resetAndAdvanceToEventId(atDivergence.meta.eventId);
  }
  // Comps before divergence should not be accounted for
  const compsBeforeDivergence = simulation.availableCompensateable();
  const compsBeforeDivergenceIndices = new Set(
    compsBeforeDivergence.map((x) => x.codeIndex)
  );

  simulation.resetAndAdvanceToEventId(fromPoint.meta.eventId);
  // advancing most canon might resolve some compensations.
  // `advanceToMostCanon` is the key function call that will eventually trigger CompensationDone
  simulation.advanceToMostCanon();
  const allActiveCompensations = simulation.availableCompensateable();

  // subtract "before-divergence" from "all" and we get compensations that we need
  const activeCompensationsBetweenFromAndTwo = Array.from(
    allActiveCompensations
  ).filter((x) => !compsBeforeDivergenceIndices.has(x.codeIndex));

  if (activeCompensationsBetweenFromAndTwo.length === 0) return null;

  return {
    compensations: activeCompensationsBetweenFromAndTwo.map((x) => ({
      /**
       * The "from" attribute is identified by the first event within the compensation block, not from the "from point".
       */
      fromTimelineOf: x.fromTimelineOf,
      toTimelineOf: toPoint.meta.eventId,
      codeIndex: x.codeIndex,
    })),
    lastMachineState: simulation,
  };
};
