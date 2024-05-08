import {
  Actyx,
  CancelSubscription,
  EventsMsg,
  EventsOrTimetravel,
  MsgType,
  OnCompleteOrErr,
  Tag,
  Tags,
} from "@actyx/sdk";

import * as Reality from "./reality.js";
import { Emit, WFMachine, WFWorkflow } from "./wfmachine.js";
export { Reality };
import { Node } from "./ax-mock/index.js";
import {
  divertedFromOtherChainAt as divertedFromOtherChainAt,
  ActyxWFBusiness,
  CTypeProto,
  InternalTag,
  WFBusinessOrMarker,
  extractWFDirective,
  extractWFEvents,
  Chain,
  WFMarker,
  WFMarkerCompensationNeeded,
  WFMarkerCompensationDone,
} from "./consts.js";
import { machine } from "os";

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
  digestedChain: ActyxWFBusiness<CType>[];
} & (
  | { t: "normal" }
  | { t: "building-compensations" }
  | {
      t: "compensating";
      canonWFMachine: WFMachine<CType>;
      canonDigestedChain: ActyxWFBusiness<CType>[];
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
  // COMPENSATE {
  //   ...
  //   COMPENSATE {
  //     // state.name & bindings
  //     // jump
  //     a <- {  } // do the machine need to compensate if the bindings are different
  //     b <-
  //   } WITH {
  //     ...
  //   }
  //   c <-
  // } WITH {
  //   ...
  // }
  //

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
      .availableCommands()
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
          const publishPromise = node.api.publish(
            tags.applyTyped({ t: x.name, payload })
          );
          return publishPromise;
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
    const nextOfMostCanonChain = (chain: Chain<CType>) => {
      const last = chain.at(chain.length - 1);
      if (!last) return multiverseTree.getCanonChain() || [];
      const chainForward = multiverseTree.getCanonChainForwards(last) || [];
      return chainForward.slice(1); // exclude `last` from the chain
    };

    const nextOfCompensateChain = (chain: Chain<CType>) => {
      const last = chain.at(chain.length - 1);
      if (!last) return multiverseTree.getCanonChain() || [];
      const chainForward =
        multiverseTree.getCompensationChainForwards(last) || [];
      return chainForward.slice(1); // exclude `last` from the chain
    };

    const multiverseTree = Reality.MultiverseTree.make<CType>();
    const compensationMap = CompensationMap.make();
    let data: DataModes<CType> = {
      t: "building-compensations",
      wfMachine: WFMachine(params.self, workflow),
      digestedChain: [],
    };

    const currentCompensation = () =>
      data.t === "compensating" ? data.compensationInfo : null;

    /**
     * There can be discrepancies between the compensations noted in the
     * CompensationMap and WFMachine because an actor can exit at the moment
     * between 1.) when the compensation is finished and 2.) when the
     * "CompensationDone" marker is published
     */
    const compareRememberedCompensation = (
      rememberedCompensation: ReturnType<
        (typeof compensationMap)["getByActor"]
      >[any]
    ) => {
      const { fromTimelineOf, toTimelineOf } = rememberedCompensation;
      const fromlastEvent = multiverseTree.getById(fromTimelineOf);
      const toLastEvent = multiverseTree.getById(toTimelineOf);
      // TODO: handle more gracefully
      if (!fromlastEvent || !toLastEvent) {
        throw new Error("missing to or from event noted in a compensation");
      }
      const fromChain =
        multiverseTree.getCompensationChainSpanning(fromlastEvent);
      const toChain = multiverseTree.getChainBackwards(toLastEvent);
      // TODO: handle more gracefully
      if (!fromChain || !toChain) {
        throw new Error("missing chain noted in a compensation");
      }

      // Do a comparison between remembered compensations and the actually needed compensations
      const compensations = calculateCompensations(
        params,
        workflow,
        fromChain,
        toChain
      );

      const matchingCompensation = (() => {
        if (!compensations) return null;
        const matchingCompensation = compensations.compensations.find(
          (x) =>
            x.fromTimelineOf === fromTimelineOf &&
            x.toTimelineOf === toTimelineOf &&
            x.codeIndex === rememberedCompensation.directive.codeIndex
        );
        if (!matchingCompensation) return null;

        return {
          compensation: matchingCompensation,
          lastMachineState: compensations.lastMachineState,
        } as const;
      })();

      if (!matchingCompensation) return null;

      return {
        matchingCompensation,
        fromChain,
        toChain,
      };
    };

    const recalc = () => {
      // predecessorMap.getBackwardChain(compensationMap.getByActor(...))
      // TODO: return null means something abnormal happens in predecessorMap e.g. missing root, missing event details
      // TODO: think about compensation events tag

      const canonWFMachine = WFMachine(params.self, workflow);
      const canonChain = multiverseTree.getCanonChain() || [];
      canonChain.map(Emit.fromWFEvent).forEach(canonWFMachine.tick);

      // TODO: examine all compensations. Not just one
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
            digestedChain: compensationComparison.fromChain,
            canonWFMachine: canonWFMachine,
            canonDigestedChain: canonChain,
            compensationInfo: rememberedCompensation.directive,
          };
        } else {
          // If the WFMachine indicates that the compensation is done when the compensationMap remembers differently
          // This is an indication that the compensation is done but unmarked
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
            digestedChain: canonChain,
          };
        }
      } else {
        data = {
          t: "normal",
          wfMachine: canonWFMachine,
          digestedChain: canonChain,
        };
      }
    };

    return {
      recalc,
      internal: () => data,
      machine: () => data.wfMachine,
      compensation: currentCompensation,
      last: () => data.digestedChain.at(data.digestedChain.length - 1),
      setToBuildingCompensation: () => {
        data = {
          t: "building-compensations",
          wfMachine: data.wfMachine,
          digestedChain: data.digestedChain,
        };
      },
      pipe: (e: EventsMsg<WFBusinessOrMarker<CType>>) => {
        extractWFEvents(e.events).map((ev) => multiverseTree.register(ev));
        extractWFDirective(e.events).map((ev) => {
          compensationMap.register(ev.payload);
        });

        const currentData = data;
        if (currentData.t === "building-compensations" && e.caughtUp) {
          const canonChain = multiverseTree.getCanonChain() || [];
          const canonWFMachine = WFMachine(params.self, workflow);
          canonChain.map(Emit.fromWFEvent).forEach(canonWFMachine.tick);

          const compensations = calculateCompensations(
            params,
            workflow,
            currentData.digestedChain,
            canonChain
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
          const nextOfChain = nextOfMostCanonChain(currentData.digestedChain);
          nextOfChain.map(Emit.fromWFEvent).forEach(currentData.wfMachine.tick);
          currentData.digestedChain.push(...nextOfChain);
          return;
        }

        if (currentData.t === "compensating") {
          const nextOfCompensateable = nextOfCompensateChain(
            currentData.digestedChain
          );
          currentData.digestedChain.push(...nextOfCompensateable);
          nextOfCompensateable
            .map(Emit.fromWFEvent)
            .forEach(currentData.wfMachine.tick);

          const nextOfCanon = nextOfMostCanonChain(
            currentData.canonDigestedChain
          );
          currentData.canonDigestedChain.push(...nextOfCanon);
          nextOfCanon
            .map(Emit.fromWFEvent)
            .forEach(currentData.canonWFMachine.tick);

          // check if compensation still applies
          // TODO: optimize compensation query
          const activeCompensationCode = currentData.wfMachine
            .activeCompensationCode()
            .findIndex(
              (x) => x.codeIndex === currentData.compensationInfo.codeIndex
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
  params: Params<CType>,
  workflow: WFWorkflow<CType>,
  previousChain: Chain<CType>,
  currentCanonChain: Chain<CType>
) => {
  const lastDigested = previousChain.at(previousChain.length - 1);
  if (!lastDigested) return null;

  const lastCanonDigested = currentCanonChain.at(currentCanonChain.length - 1);
  if (!lastCanonDigested) return null;

  const divergence = divertedFromOtherChainAt(previousChain, currentCanonChain);

  if (divergence === previousChain.length) return null;

  const simulation = WFMachine(params.self, workflow);

  const beforeDivergence = previousChain.slice(0, divergence);
  const afterDivergence = previousChain.slice(divergence);

  beforeDivergence.map(Emit.fromWFEvent).forEach(simulation.tick);

  const invalidCompensations = simulation.activeCompensationCode();
  const invalidCompensationIndices = new Set(
    invalidCompensations.map((x) => x.codeIndex)
  );

  afterDivergence.map(Emit.fromWFEvent).forEach(simulation.tick);

  const allCompensations = simulation.activeCompensationCode();

  const validCompensations = Array.from(allCompensations).filter(
    (x) => !invalidCompensationIndices.has(x.codeIndex)
  );

  if (validCompensations.length === 0) return null;

  return {
    compensations: validCompensations.map((x) => ({
      fromTimelineOf: x.firstEventId,
      toTimelineOf: lastCanonDigested.meta.eventId,
      codeIndex: x.codeIndex,
    })),
    lastMachineState: simulation,
  };
};
