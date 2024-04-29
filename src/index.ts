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

// TODO: build compensation tracker here
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

  /**
   * Connect to Actyx with predefined params and keep connection until closed.
   */

  // let nextCanonEventIndexToSwallow = 0;
  // let witnessCanon: null | Reality.Reality<ActyxWFEvent<CType>> = null;
  type DataModes = {
    wfMachine: WFMachine<CType>;
    digestedChain: ActyxWFEvent<CType>[];
  } & (
    | {
        t: "normal";
      }
    | {
        t: "building-amendments";
      }
    | {
        t: "amending";
        nextWFMachine: WFMachine<CType>;
        nextDigestedChain: ActyxWFEvent<CType>[];
      }
  );
  let data: DataModes = {
    t: "normal",
    wfMachine: WFMachine(workflow),
    digestedChain: [],
  };
  let alive = false;

  const pipeEventsToWFMachine = (
    e: EventsOrTimetravel<WFEventOrDirective<CType>>
  ) => {
    const currentData = data;

    if (e.type === MsgType.timetravel) {
      data = {
        t: "building-amendments",
        wfMachine: currentData.wfMachine,
        digestedChain: currentData.digestedChain,
      };
      return;
    }

    if (e.type === MsgType.events) {
      if (currentData.t === "normal") {
        const nextInChain = (() => {
          const last = data.digestedChain.at(data.digestedChain.length - 1);
          if (!last) return multiverseTree.getCanonChain() || [];
          const chainForward = multiverseTree.getChainForwards(last) || [];
          return chainForward.slice(1); // exclude `last` from the chain
        })();

        nextInChain.map(Emit.fromWFEvent).forEach(data.wfMachine.tick);
        data.digestedChain.push(...nextInChain);
      } else if (currentData.t === "building-amendments" && e.caughtUp) {
        const nextChain = multiverseTree.getCanonChain() || [];
        const compensations = calculateCompensation(
          workflow,
          data.digestedChain,
          nextChain
        );

        const nextWFMachine = WFMachine(workflow);
        nextChain.map(Emit.fromWFEvent).forEach(nextWFMachine.tick);

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
              node.api.publish(Tag("sometag").apply(directive));
              compensationMap.register(directive);
            }
          );

          data = {
            t: "amending",
            wfMachine: currentData.wfMachine,
            digestedChain: currentData.digestedChain,
            nextWFMachine: nextWFMachine,
            nextDigestedChain: nextChain,
          };
        } else {
          data = {
            t: "normal",
            wfMachine: nextWFMachine,
            digestedChain: nextChain,
          };
        }
      } else if (currentData.t === "amending") {
        const nextInChain = (() => {
          const last = currentData.nextDigestedChain.at(
            data.digestedChain.length - 1
          );
          if (!last) return multiverseTree.getCanonChain() || [];
          const chainForward = multiverseTree.getChainForwards(last) || [];
          return chainForward.slice(1); // exclude `last` from the chain
        })();

        nextInChain
          .map(Emit.fromWFEvent)
          .forEach(currentData.nextWFMachine.tick);
        currentData.nextDigestedChain.push(...nextInChain);
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
        throw new Error("missing chain noted in a compensation");
      }
      const compensateableMachine = WFMachine(workflow);
      compensateableChain
        .map(Emit.fromWFEvent)
        .forEach(compensateableMachine.tick);

      data = {
        t: "amending",
        wfMachine: compensateableMachine,
        digestedChain: compensateableChain,
        nextWFMachine: canonWFMachine,
        nextDigestedChain: canonChain,
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

  const commands = () =>
    data.wfMachine
      .availableCommands()
      .filter((x) => x.role === params.self)
      .map((x) => ({
        info: x,
        publish: (payload: Record<string, unknown>) =>
          node.api.publish(
            params.tags
              .and(
                Tag<WFEventOrDirective<CType>>(
                  InternalTag.Predecessor.write("")
                )
              ) // TODO: track predecessor
              .applyTyped({ t: x.name, payload })
          ),
      }));

  return {
    amendment: () => {
      if (!data) {
        return null;
      }
      // TODO: track this using active compensation rather than compensatable
      return {
        compensations: data.wfMachine
          .availableCompensateable()
          .filter((x) => x.role === params.self),
      };
    },
    commands,
    command: (name: CType["ev"]) =>
      commands().find((x) => x.info.name === name) || null,
    kill: async () => {
      alive = false;
      axSub.kill();
    },
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
  type CodeIndex = number;

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

const calculateCompensation = <CType extends CTypeProto>(
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
