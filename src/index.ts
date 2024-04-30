import {
  Actyx,
  CancelSubscription,
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
  ActyxWFEvent,
  CTypeProto,
  InternalTag,
  WFEventOrDirective,
  extractWFDirective,
  extractWFEvents,
  Chain,
  WFDirective,
  WFDirectiveCompensationNeeded,
  WFDirectiveCompensationDone,
} from "./consts.js";

export type Params<CType extends CTypeProto> = {
  actyx: Parameters<(typeof Actyx)["of"]>;
  tags: Tags<WFEventOrDirective<CType>>;
  self: CType["role"];
  id: string;
};

type OnEventsOrTimetravel<E> = (data: EventsOrTimetravel<E>) => Promise<void>;

export const run = async <CType extends CTypeProto>(
  params: Params<CType>,
  // const node = Node.make<WFEventAndDirective<CType>>({ id: params.id });
  node: Node.Type<WFEventOrDirective<CType>>,
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

  let caughtUpFirstTime = false;
  const multiverseTree = Reality.MultiverseTree.make<CType>();
  const compensationMap = CompensationMap.make();

  type DataModes = {
    wfMachine: WFMachine<CType>;
    digestedChain: ActyxWFEvent<CType>[];
  } & (
    | { t: "normal" }
    | { t: "building-compensations" }
    | {
        t: "compensating";
        canonWFMachine: WFMachine<CType>;
        canonDigestedChain: ActyxWFEvent<CType>[];
        compensationInfo: WFDirectiveCompensationNeeded;
      }
  );
  let data: DataModes = {
    t: "normal",
    wfMachine: WFMachine(workflow),
    digestedChain: [],
  };

  const nextOfMostCanonChain = (chain: Chain<CType>) => {
    const last = chain.at(chain.length - 1);
    if (!last) return multiverseTree.getCanonChain() || [];
    const chainForward = multiverseTree.getChainForwards(last) || [];
    return chainForward.slice(1); // exclude `last` from the chain
  };

  const nextOfCompensateChain = (chain: Chain<CType>) => {
    const last = chain.at(chain.length - 1);
    if (!last) return multiverseTree.getCanonChain() || [];
    const chainForward =
      multiverseTree.getCompensationChainForwards(last) || [];
    return chainForward.slice(1); // exclude `last` from the chain
  };

  const pipeEventsToWFMachine = (
    e: EventsOrTimetravel<WFEventOrDirective<CType>>
  ) => {
    const currentData = data;

    if (e.type === MsgType.timetravel) {
      data = {
        t: "building-compensations",
        wfMachine: currentData.wfMachine,
        digestedChain: currentData.digestedChain,
      };
      return;
    }

    if (e.type === MsgType.events) {
      if (currentData.t === "normal") {
        const nextOfChain = nextOfMostCanonChain(data.digestedChain);
        nextOfChain.map(Emit.fromWFEvent).forEach(data.wfMachine.tick);
        data.digestedChain.push(...nextOfChain);
      } else if (currentData.t === "building-compensations" && e.caughtUp) {
        const canonChain = multiverseTree.getCanonChain() || [];
        const canonWFMachine = WFMachine(workflow);
        canonChain.map(Emit.fromWFEvent).forEach(canonWFMachine.tick);

        const compensations = calculateCompensations(
          workflow,
          data.digestedChain,
          canonChain
        );

        if (compensations) {
          // register compensations to both compensation map and the persistence
          // layer: ax
          compensations.forEach(
            ({ fromTimelineOf, toTimelineOf, codeIndex }) => {
              const directive: WFDirective = {
                ax: InternalTag.CompensationNeeded.write(""),
                actor: params.id,
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

        rebuildData();
      } else if (currentData.t === "compensating") {
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
      }
    }
  };

  const rebuildData = () => {
    // TODO: calculate compensation here
    // predecessorMap.getBackwardChain(compensationMap.getByActor(...))
    // TODO: return null means something abnormal happens in predecessorMap e.g. missing root, missing event details
    // TODO: think about compensation events tag
    const compensation = compensationMap
      .getByActor(params.id)
      .sort((a, b) => b.directive.codeIndex - a.directive.codeIndex)
      .at(0);

    const canonWFMachine = WFMachine(workflow);
    const canonChain = multiverseTree.getCanonChain() || [];
    canonChain.map(Emit.fromWFEvent).forEach(canonWFMachine.tick);

    if (compensation) {
      const compensateableLastEvent = multiverseTree.getById(
        compensation.fromTimelineOf
      );
      if (!compensateableLastEvent) {
        // TODO: handle more gracefully
        throw new Error("missing last event noted in a compensation");
      }
      const compensateableChain = multiverseTree.getChainBackwards(
        compensateableLastEvent
      );
      if (!compensateableChain) {
        // TODO: handle more gracefully
        throw new Error("missing chain noted in a compensation");
      }

      compensateableChain.push(...nextOfCompensateChain(compensateableChain));

      const compensateableMachine = WFMachine(workflow);
      compensateableChain
        .map(Emit.fromWFEvent)
        .forEach(compensateableMachine.tick);

      data = {
        t: "compensating",
        wfMachine: compensateableMachine,
        digestedChain: compensateableChain,
        canonWFMachine: canonWFMachine,
        canonDigestedChain: canonChain,
        compensationInfo: compensation.directive,
      };
    } else {
      data = {
        t: "normal",
        wfMachine: canonWFMachine,
        digestedChain: canonChain,
      };
    }
  };

  const axSub = perpetualSubscription(node, async (e) => {
    if (e.type === MsgType.events) {
      extractWFEvents(e.events).map((ev) => multiverseTree.register(ev));
      extractWFDirective(e.events).map((ev) => {
        compensationMap.register(ev.payload);
      });
    }

    if (!caughtUpFirstTime) {
      if (e.type === MsgType.events) {
        if (e.caughtUp) {
          caughtUpFirstTime = true;
          rebuildData();
        }
      }
    } else {
      pipeEventsToWFMachine(e);
    }
  });

  const commands = () => {
    if (data.t === "building-compensations") {
      return [];
    }

    const commands = data.wfMachine
      .availableCommands()
      .filter((x) => x.role === params.self)
      .map((x) => ({
        info: x,
        publish: (payload: Record<string, unknown>) => {
          const last = data.digestedChain.at(data.digestedChain.length - 1);
          let tags = params.tags;
          if (last) {
            tags = tags.and(
              Tag<WFEventOrDirective<CType>>(
                InternalTag.Predecessor.write(last.meta.eventId)
              )
            );
          }
          if (data.t === "compensating") {
            tags = tags.and(
              Tag<WFEventOrDirective<CType>>(
                InternalTag.CompensationEvent.write(
                  data.compensationInfo.fromTimelineOf
                )
              )
            );
          }
          return node.api.publish(tags.applyTyped({ t: x.name, payload }));
        },
      }));

    if (data.t === "compensating") {
      return commands.filter((x) => x.info.reason === "compensation");
    }

    return commands;
  };

  return {
    commands,
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

// TODO: fix tracking, track compensation indicies instead
export namespace CompensationMap {
  type Actor = string;
  type From = string;
  type To = string;

  export const make = () => {
    const data = {
      positive: new Map<
        Actor,
        Map<From, Map<To, WFDirectiveCompensationNeeded>>
      >(),
      negative: new Map<Actor, Map<From, Map<To, boolean>>>(),
    };

    const access = <T>(
      entry: Map<Actor, Map<From, Map<To, T>>>,
      { actor, fromTimelineOf: from }: WFDirective
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
      register: (compensation: WFDirective) => {
        // TODO: runtime validation
        if (InternalTag.CompensationNeeded.is(compensation.ax)) {
          const needed = compensation as WFDirectiveCompensationNeeded;
          const set = access(data.positive, compensation);
          set.set(needed.toTimelineOf, needed);
        } else if (InternalTag.CompensationDone.is(compensation.ax)) {
          const done = compensation as WFDirectiveCompensationDone;
          const set = access(data.negative, compensation);
          set.delete(done.toTimelineOf);
        }
      },
      getByActor: (actor: string) => {
        const ret = [] as {
          fromTimelineOf: string;
          toTimelineOf: string;
          directive: WFDirectiveCompensationNeeded;
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
  previousChain: Chain<CType>,
  currentCanonChain: Chain<CType>
) => {
  const lastDigested = previousChain.at(previousChain.length - 1);
  if (!lastDigested) return null;

  const lastCanonDigested = currentCanonChain.at(currentCanonChain.length - 1);
  if (!lastCanonDigested) return null;

  const divergence = divertedFromOtherChainAt(previousChain, currentCanonChain);

  if (divergence === previousChain.length) return null;

  const simulation = WFMachine(workflow);

  const beforeDivergence = previousChain.slice(0, divergence);
  const afterDivergence = previousChain.slice(divergence);

  beforeDivergence.map(Emit.fromWFEvent).forEach(simulation.tick);

  const invalidCompensations = simulation.availableCompensateableCode();
  const invalidCompensationIndices = new Set(
    invalidCompensations.map((x) => x.codeIndex)
  );

  afterDivergence.map(Emit.fromWFEvent).forEach(simulation.tick);

  const allCompensations = simulation.availableCompensateableCode();

  const validCompensations = Array.from(allCompensations).filter(
    (x) => !invalidCompensationIndices.has(x.codeIndex)
  );

  if (validCompensations.length === 0) return null;

  return validCompensations.map((x) => ({
    fromTimelineOf: x.firstEventId,
    toTimelineOf: lastCanonDigested.meta.eventId,
    codeIndex: x.codeIndex,
  }));
};
