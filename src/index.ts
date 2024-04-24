import {
  Actyx,
  ActyxEvent,
  CancelSubscription,
  EventsOrTimetravel,
  MsgType,
  OnCompleteOrErr,
  Tag,
  Tags,
} from "@actyx/sdk";

import * as Reality from "./reality.js";
import { Emit, WFMachine, WFWorkflow, statesAreEqual } from "./wfmachine.js";
import { NumArrToCodepoint, ExcludeArrayMember, sleep } from "./utils.js";
export { Reality };
import { Node } from "./ax-mock/index.js";
import { ERPromise } from "systemic-ts-utils";
import {
  ActyxWFEvent,
  ActyxWFEventAndDirective as ActyxWFEventAndDirective,
  CTypeProto,
  InternalTag,
  WFDirective,
  WFEventOrDirective,
  extractWFEvents,
  isWFEvent,
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
  type TypedWitness = Reality.Witness<ActyxWFEvent<CType>>;
  type TypedWitnessBuilder = WitnessBuilder.Type<CType>;

  let witnessOrBuilder: TypedWitnessBuilder | TypedWitness =
    WitnessBuilder.make<CType>({
      id: (e) => e.meta.eventId,
    });

  /**
   * This is an async barrier that will be resolved when builder is finished building.
   */
  const rebuildingBarrier = ERPromise.ERPromise.make<TypedWitness>();

  /**
   * Connect to Actyx with predefined params and keep connection until closed.
   */

  // let nextCanonEventIndexToSwallow = 0;
  // let witnessCanon: null | Reality.Reality<ActyxWFEvent<CType>> = null;
  type DataModes = { wfMachine: WFMachine<CType> } & (
    | {
        t: "normal";
      }
    | {
        t: "building-amendments";
      }
    | {
        t: "amending";
        nextWFMachine: WFMachine<CType>;
      }
  );
  let data: DataModes = {
    t: "normal",
    wfMachine: WFMachine(workflow),
  };
  let alive = false;

  const actyxEventHandler = {
    whenStartingUp: (
      builder: TypedWitnessBuilder,
      e: EventsOrTimetravel<WFEventOrDirective<CType>>
    ) => {
      if (e.type !== MsgType.events) return;
      extractWFEvents(e.events).map((ev) => builder.push(ev));
      if (e.caughtUp) {
        const { witness } = builder.build();
        witnessOrBuilder = witness;
        rebuildingBarrier.control.resolve(witness);
      }
    },
    whenStarted: (
      witness: TypedWitness,
      e: EventsOrTimetravel<WFEventOrDirective<CType>>
    ) => {
      // piping events to witness
      if (e.type === MsgType.events) {
        extractWFEvents(e.events).map((ev) => witness.see(ev));
      } else if (e.type === MsgType.timetravel) {
        witness.retrospect();
      }

      pipeEventsToWFMachine(witness, e);
    },
  };

  const pipeEventsToWFMachine = (
    witness: TypedWitness,
    e: EventsOrTimetravel<WFEventOrDirective<CType>>
  ) => {
    const currentData = data;

    if (e.type === MsgType.timetravel) {
      data = {
        t: "building-amendments",
        wfMachine: currentData.wfMachine,
      };
    } else if (e.type === MsgType.events) {
      if (currentData.t === "normal") {
        extractWFEvents(e.events).map(({ payload }) =>
          currentData.wfMachine.tick(Emit.event(payload.t, payload.payload))
        );
      } else if (currentData.t === "building-amendments" && e.caughtUp) {
        // TODO: should we use divergentPoint analysis here?
        const nextWFMachine = WFMachine(workflow);
        witness
          .canonReality()
          .history()
          .chain.map(([_, { payload }]) =>
            nextWFMachine.tick(Emit.event(payload.t, payload.payload))
          );

        // determine compensations
        const compensations = (() => {
          const stateOfNextAndPrevAreEqual = statesAreEqual(
            nextWFMachine.state(),
            currentData.wfMachine.state()
          );
          if (stateOfNextAndPrevAreEqual) return null;

          const availableCompensations =
            currentData.wfMachine.availableCompensateable();
          if (availableCompensations.length === 0) return null;
          return availableCompensations;
        })();

        if (!compensations) {
          data = {
            t: "normal",
            wfMachine: nextWFMachine,
          };
        } else {
          data = {
            t: "amending",
            wfMachine: currentData.wfMachine,
            nextWFMachine: nextWFMachine,
          };
        }
      } else if (currentData.t === "amending") {
        // keep next WFMachine updated as we're going about amendments
        extractWFEvents(e.events).map(({ payload }) =>
          currentData.nextWFMachine.tick(Emit.event(payload.t, payload.payload))
        );
      }
    }
  };

  const axSub = perpetualSubscription(node, async (e) => {
    if ("build" in witnessOrBuilder) {
      return actyxEventHandler.whenStartingUp(witnessOrBuilder, e);
    } else {
      return actyxEventHandler.whenStarted(witnessOrBuilder, e);
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

export namespace WitnessBuilder {
  export type Type<CType extends CTypeProto> = {
    push: (x: ActyxWFEventAndDirective<CType>) => unknown;
    build: () => {
      witness: Reality.Witness<ActyxWFEvent<CType>>;
    };
  };

  type EventId = string;
  type EventInfo<CType extends CTypeProto> = {
    ev: ActyxWFEvent<CType>;
    index: number;
  };
  /**
   * Contains intermediate data structures needed to build a witness and
   * multiple realities
   */
  type Intermediate<CType extends CTypeProto> = {
    infoMap: Map<EventId, EventInfo<CType>>;
    /**
     * maps an event to its predecessor
     * read as: SecondEventId is the predecessor of FirstEventId
     */
    predecessorMap: Map<EventId, EventId>;
    compensationMap: {
      data: { actor: string; at: EventId }[];
      add: (actor: string, at: EventId) => unknown;
      remove: (actor: string, at: EventId) => unknown;
    };
  };

  const buildIntermediate = <CType extends CTypeProto>(
    history: ActyxWFEventAndDirective<CType>[]
  ): Intermediate<CType> => {
    const infoMap = new Map<EventId, EventInfo<CType>>();
    const predecessorMap = new Map<EventId, EventId>(); // mapping an event to its predecessor
    const compensationMap = {
      data: [] as { actor: string; at: EventId }[],
      add: (actor: string, at: EventId) =>
        compensationMap.data.push({ actor, at }),
      remove: (actor: string, at: EventId) =>
        (compensationMap.data = compensationMap.data.filter(
          (x) => x.actor === actor && x.at === at
        )),
    };

    const pushWFEvent = (ev: ActyxWFEvent<CType>, index: number) => {
      infoMap.set(ev.meta.eventId, { ev, index });
      const predecessorId = ev.meta.tags.find((x) =>
        InternalTag.Predecessor.read(x)
      );
      if (!predecessorId) return;
      predecessorMap.set(ev.meta.eventId, predecessorId);
    };

    const pushWFDirective = (directive: WFDirective) => {
      if (InternalTag.CompensationNeeded.is(directive.ax)) {
        compensationMap.add(directive.actor, directive.at);
      } else if (InternalTag.CompensationFinished.is(directive.ax)) {
        compensationMap.remove(directive.actor, directive.at);
      }
    };

    history.forEach((ev, index) => {
      const { meta, payload } = ev;
      // TODO: Runtime validator
      if (isWFEvent(payload)) {
        return pushWFEvent({ meta, payload }, index);
      } else {
        return pushWFDirective(payload);
      }
    });

    return { infoMap, predecessorMap, compensationMap };
  };

  export const make = <CType extends CTypeProto>(
    ...witnessParams: Parameters<typeof Reality.witness<ActyxWFEvent<CType>>>
  ): Type<CType> => {
    type Self = Type<CType>;

    const history = [] as ActyxWFEventAndDirective<CType>[];

    // methods

    const push: Self["push"] = (x) => history.push(x);
    const build: Self["build"] = () => {
      // given these events in history:
      //
      // [0] a [first-event]
      // [1] z [first-event]
      // [2] a->b [last-event]
      // [3] a->c
      // [4] c->d
      // [5] z->y [last-event]
      // [6] d->e [last-event]
      // [7] c->f [last-event]
      //
      // results in sorted chains:
      //
      // a->b [0,2] // meaning the chain's index value is 0,2, index value is used to sort. radix?
      // a->c->d->e [0,3,4,6]
      // a->c->d->f [0,3,4,7]
      // z->y [1,5]

      const { infoMap, predecessorMap, compensationMap } =
        buildIntermediate(history);

      const lastEvents = (() => {
        const predecessors = new Set(predecessorMap.values());
        return Array.from(predecessorMap.keys()).filter(
          (id) => !predecessors.has(id)
        );
      })();

      // build chains
      const chains = lastEvents
        .map((x) => {
          const idChain = [x]; // chronological event id chains
          while (true) {
            const predecessor = predecessorMap.get(idChain[0]) || null;
            if (predecessor === null) break;
            idChain.unshift(predecessor);
          }

          const infoChain = idChain.map((x) => infoMap.get(x) || null);
          // get rid of abnormal tags or incomplete chain due to malformed query or deleted events
          if (infoChain.findIndex((x) => x === null) !== -1) return null;

          return infoChain as ExcludeArrayMember<typeof infoChain, null>;
        })
        .filter((x): x is EventInfo<CType>[] => x !== null)
        .map((infoChain) => ({
          chain: infoChain.map((x) => x.ev),
          sortKey: infoChain.map((x) => x.index), // trick to make it easy to
        }));

      const sortedChains = chains
        .map(({ chain, sortKey }) => ({
          chain,
          sortKey: NumArrToCodepoint.nts(sortKey), // trick to make it easy to sort array of number as a string
        }))
        .sort(({ sortKey: a }, { sortKey: b }) => {
          if (a > b) return 1;
          if (a < b) return -1;
          return 0;
        })
        .map((x) => x.chain);

      const witness = Reality.witness(...witnessParams);

      // the expired goes first into the witness
      sortedChains.reverse().forEach((chain, index) => {
        chain.forEach((ev) => witness.see(ev));
        if (index < sortedChains.length - 1) {
          witness.retrospect();
        }
      });

      return { witness };
    };

    // the builder
    return { push, build };
  };
}
