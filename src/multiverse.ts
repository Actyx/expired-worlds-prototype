import {
  ActyxWFBusiness,
  ActyxWFCanonDecide,
  ActyxWFCanonMarker,
  ActyxWFCanonAdvrt,
  CTypeProto,
  InternalTag,
  sortByEventKey,
  isWFCanonAdvrtMarker,
  isWFCanonDecideMarker,
} from "./consts.js";
import { Logger, makeLogger, MultihashMap } from "./utils.js";

type EventId = string;

export namespace MultiverseTree {
  export const UnregisteredEvent: unique symbol = Symbol("UnregisteredEvent");
  export type UnregisteredEvent = typeof UnregisteredEvent;
  export type Type<CType extends CTypeProto> = {
    register: (e: ActyxWFBusiness<CType>) => void;
    depthOf: (eventId: string) => number;
    /**
     * In the case of parallels, there can be more than one predecessors
     */
    getPredecessors: (
      event: string
    ) => (ActyxWFBusiness<CType> | UnregisteredEvent)[];
    has: (e: ActyxWFBusiness<CType>) => boolean;
    getNextById: (e: string) => ActyxWFBusiness<CType>[];
    getById: (id: string) => null | ActyxWFBusiness<CType>;
    isHead: (e: ActyxWFBusiness<CType>) => boolean;
    isRoot: (e: ActyxWFBusiness<CType>) => boolean;
    getRoots: () => ActyxWFBusiness<CType>[];
  };

  export const make = <CType extends CTypeProto>(): Type<CType> => {
    const internal = {
      infoMap: new Map<EventId, ActyxWFBusiness<CType>>(),
      predecessors: new Map<EventId, Set<EventId>>(),
      next: new Map<EventId, Set<EventId>>(),
      depth: new Map<EventId, number>(),
    };

    const predecessorSetOf = (eventId: string) => {
      const predecessorSet = internal.predecessors.get(eventId) || new Set();
      internal.predecessors.set(eventId, predecessorSet);
      return predecessorSet;
    };

    const nextSetOf = (eventId: string) => {
      const nextSet = internal.next.get(eventId) || new Set();
      internal.next.set(eventId, nextSet);
      return nextSet;
    };

    const self: Type<CType> = {
      register: (ev) => {
        const { eventId } = ev.meta;
        if (internal.infoMap.has(eventId)) return;
        internal.infoMap.set(eventId, ev);
        const predecessorSet = predecessorSetOf(eventId);

        const predecessorIds = ev.meta.tags
          .map((x) => InternalTag.Predecessor.read(x))
          .filter((x): x is Exclude<typeof x, null> => x !== null);

        predecessorIds.forEach((predecessorId) => {
          predecessorSet.add(predecessorId);
          nextSetOf(predecessorId).add(eventId);
        });

        const depth =
          predecessorIds.reduce(
            (max, id) => Math.max(max, internal.depth.get(id) || 0),
            0
          ) + 1;

        internal.depth.set(eventId, depth);
      },
      depthOf: (eventId: string) => internal.depth.get(eventId) || 0,
      getRoots: () => Array.from(internal.infoMap.values()).filter(self.isRoot),
      getById: (id) => internal.infoMap.get(id) || null,
      getNextById: (id) =>
        Array.from(nextSetOf(id))
          .map((eventId) => internal.infoMap.get(eventId) || null)
          .filter((ev): ev is Exclude<typeof ev, null> => ev !== null),
      has: (ev) => internal.infoMap.has(ev.meta.eventId),
      getPredecessors: (eventId) =>
        Array.from(predecessorSetOf(eventId))
          .map((x) => internal.infoMap.get(x) || UnregisteredEvent)
          .filter((x): x is Exclude<typeof x, null> => x !== null),
      isHead: (ev) => nextSetOf(ev.meta.eventId).size === 0,
      isRoot: (ev) => predecessorSetOf(ev.meta.eventId).size === 0,
    };

    return self;
  };
}

export namespace CanonizationStore {
  export type Type<CType extends CTypeProto> = {
    logger: Logger;
    register: (input: ActyxWFCanonMarker<CType>) => void;
    getOpenAdvertisements: () => ActyxWFCanonAdvrt<CType>[];
    getAdvertisementsForAddress: (
      name: CType["ev"],
      depth: number
    ) => ActyxWFCanonAdvrt<CType>[];
    getDecisionsForAddress: (
      name: CType["ev"],
      depth: number
    ) => ActyxWFCanonDecide<CType>[];
    /**
     * Sorted from the most present
     */
    listDecisionsFromLatest: () => ActyxWFCanonDecide<CType>[];
  };

  // TODO optimize API design
  export const make = <CType extends CTypeProto>(): Type<CType> => {
    type Name = string;
    type EventId = string;
    type Advrt = ActyxWFCanonAdvrt<CType>;
    type Decision = ActyxWFCanonDecide<CType>;
    type Depth = number;

    const logger = makeLogger("CanonizationStore");
    const advertisements = MultihashMap.depth(2).make<
      [Name, Depth],
      Map<EventId, Advrt>
    >();

    const decisions = MultihashMap.depth(2).make<
      [Name, Depth],
      Map<EventId, Decision>
    >();
    let decisionsFromLatest = [] as Decision[];

    const access = <M extends MultihashMap.Type<any, any, any>>(
      map: M,
      key: MultihashMap.KeyOf<M>,
      def: () => MultihashMap.ValueOf<M>
    ): MultihashMap.ValueOf<M> => {
      const val: MultihashMap.ValueOf<M> = map.get(key) || def();
      if (!map.has(key)) map.set(key, val);
      return val;
    };

    const self: Type<CType> = {
      logger,
      register: (input) => {
        const {
          meta: { eventId },
          payload: { name, depth },
        } = input;

        if (isWFCanonAdvrtMarker(input.payload)) {
          const advrt = input as ActyxWFCanonAdvrt<CType>;
          const eventmap = access(
            advertisements,
            [name, depth],
            () => new Map()
          );
          eventmap.set(eventId, advrt);
        } else if (isWFCanonDecideMarker(input.payload)) {
          const decision = input as ActyxWFCanonDecide<CType>;
          const eventmap = access(decisions, [name, depth], () => new Map());
          eventmap.set(eventId, decision);

          decisionsFromLatest = sortByEventKey([
            ...decisionsFromLatest,
            decision,
          ]).reverse();
        }
      },
      listDecisionsFromLatest: () => decisionsFromLatest,
      getAdvertisementsForAddress: (name, depth) => {
        const ads = advertisements.get([name, depth]);
        if (!ads) return [];
        return sortByEventKey(Array.from(ads.values())).reverse();
      },
      getDecisionsForAddress: (name, depth) => {
        const dec = decisions.get([name, depth]);
        if (!dec) return [];
        return sortByEventKey(Array.from(dec.values())).reverse();
      },
      getOpenAdvertisements: () => {
        const allAdNumMap = Array.from(advertisements.values()).flatMap((x) =>
          Array.from(x.values())
        );

        const res = allAdNumMap.filter((x) => {
          const eventmap = decisions.get([x.payload.name, x.payload.depth]);
          if (!eventmap) return true;
          if (eventmap.size === 0) return true;
          return false;
        });

        return res;
      },
    };
    return self;
  };
}
