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
import { Emit, WFMachine, WFWorkflow, statesAreEqual } from "./wfmachine.js";
export { Reality };
import { Node } from "./ax-mock/index.js";
import { ERPromise } from "systemic-ts-utils";
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

  const actyxEventHandler = {
    whenNotCaughtUp: (e: EventsOrTimetravel<WFEventOrDirective<CType>>) => {
      if (e.type === MsgType.events) {
        extractWFEvents(e.events).map((ev) => multiverseTree.register(ev));
        extractWFDirective(e.events).map((ev) => {
          if (InternalTag.CompensationNeeded.is(ev.payload.ax)) {
            compensationMap.insert(ev.payload.actor, ev.payload.fromTimelineOf);
            return;
          }

          if (InternalTag.CompensationDone.is(ev.payload.ax)) {
            compensationMap.delete(ev.payload.actor, ev.payload.fromTimelineOf);
            return;
          }
        });
        if (e.caughtUp) {
          caughtUpFirstTime = true;
          // TODO: calculate compensation here
          // predecessorMap.getBackwardChain(compensationMap.getByActor(...))
          // TODO: return null means something abnormal happens in predecessorMap e.g. missing root, missing event details
          const canonChain = multiverseTree.getCanonChain() || [];
          canonChain.forEach((ev) => {
            data.wfMachine.tick(Emit.event(ev.payload.t, ev.payload.payload));
            data.digestedChain.push(ev);
          });
        }
      }
    },
    whenCaughtUp: (e: EventsOrTimetravel<WFEventOrDirective<CType>>) => {
      if (e.type === MsgType.events) {
        extractWFEvents(e.events).map((ev) => multiverseTree.register(ev));
      }

      pipeEventsToWFMachine(e);
    },
  };

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
        extractWFEvents(e.events).map((ev) => {
          data.wfMachine.tick(Emit.event(ev.payload.t, ev.payload.payload));
          data.digestedChain.push(ev);
        });
      } else if (currentData.t === "building-amendments" && e.caughtUp) {
        // TODO: should we use divergentPoint analysis here?

        // recalculate previous wf machine

        // witness
        //   .canonReality()
        //   .history()
        //   .chain.map(([_, { payload }]) =>
        //     nextWFMachine.tick(Emit.event(payload.t, payload.payload))
        //   );

        // determine compensations and produce compensation notice
        // what is a compensation notice
        // it is an entry of compensation list

        const nextChain = multiverseTree.getCanonChain() || [];
        const compensations = calculateCompensation(
          workflow,
          data.digestedChain,
          nextChain
        );

        const nextWFMachine = WFMachine(workflow);
        nextChain.forEach((x) =>
          nextWFMachine.tick(Emit.event(x.payload.t, x.payload))
        );

        if (compensations) {
          const directive: WFDirective = {
            ax: InternalTag.CompensationNeeded.write(""),
            actor: params.id,
            fromTimelineOf: compensations.fromTimelineOf,
            toTimelineOf: compensations.toTimelineOf,
            compensationIndices: compensations.compensationIndices,
          };
          // TODO: fix tag
          node.api.publish(Tag("sometag").apply(directive));
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
        // keep next WFMachine updated as we're going about amendments
        extractWFEvents(e.events).map((ev) => {
          currentData.nextWFMachine.tick(
            Emit.event(ev.payload.t, ev.payload.payload)
          );
          currentData.nextDigestedChain.push(ev);
        });
      }
    }
  };

  const axSub = perpetualSubscription(node, async (e) => {
    if (!caughtUpFirstTime) {
      actyxEventHandler.whenNotCaughtUp(e);
    } else {
      actyxEventHandler.whenCaughtUp(e);
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
  type ActorId = string;
  type EventId = string;
  export const make = () => {
    const data = {
      map: new Map<ActorId, Set<EventId>>(),
      anti: new Map<ActorId, Set<EventId>>(),
    };
    return {
      insert: (actor: string, eventId: string) => {
        const entries = data.map.get(actor) || new Set();
        data.map.set(actor, entries);
        entries.add(eventId);
      },
      delete: (actor: string, eventId: string) => {
        const entries = data.anti.get(actor) || new Set();
        data.anti.set(actor, entries);
        entries.add(eventId);
      },
      getByActor: (actor: string): string[] => {
        const positive = data.map.get(actor) || new Set();
        const negative = data.anti.get(actor) || new Set();
        return Array.from(positive).filter((eventId) => !negative.has(eventId));
      },
    };
  };
}

const calculateCompensation = <CType extends CTypeProto>(
  workflow: WFWorkflow<CType>,
  previousChain: Chain<CType>,
  nextChain: Chain<CType>
) => {
  const lastDigested = previousChain.at(previousChain.length - 1);
  if (!lastDigested) return null;

  const lastNextDigested = nextChain.at(nextChain.length - 1);
  if (!lastNextDigested) return null;

  const divergence = divertedFromOtherChainAt(previousChain, nextChain);

  const beforeDivergence = previousChain.slice(0, divergence);
  const afterDivergence = previousChain.slice(divergence);

  const simulation = WFMachine(workflow);

  beforeDivergence.forEach((ev) =>
    simulation.tick(Emit.event(ev.payload.t, ev.payload.payload))
  );

  const invalidCompensationSet = simulation.availableCompensateableRaw();

  afterDivergence.forEach((ev) =>
    simulation.tick(Emit.event(ev.payload.t, ev.payload.payload))
  );

  const allCompensationIndices = simulation.availableCompensateableRaw();

  const validCompensationIndices = Array.from(allCompensationIndices).filter(
    (x) => !invalidCompensationSet.has(x)
  );

  return {
    simulation,
    fromTimelineOf: lastDigested.meta.eventId,
    toTimelineOf: lastNextDigested.meta.eventId,
    compensationIndices: validCompensationIndices,
  };
};
