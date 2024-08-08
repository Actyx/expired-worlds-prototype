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

type EventId = string;

export namespace MultiverseTree {
  export const UnregisteredEvent: unique symbol = Symbol("UnregisteredEvent");
  export type UnregisteredEvent = typeof UnregisteredEvent;
  export type Type<CType extends CTypeProto> = {
    register: (e: ActyxWFBusiness<CType>) => void;
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
      },
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
    register: (input: ActyxWFCanonMarker<CType>) => void;
    getOpenAdvertisements: () => ActyxWFCanonAdvrt<CType>[];
    getAdvertisementsForName: (name: string) => ActyxWFCanonAdvrt<CType>[];
    getDecisionsForName: (name: string) => ActyxWFCanonDecide<CType>[];
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

    const advertisements = new Map<Name, Map<EventId, Advrt>>();
    const decisions = new Map<Name, Map<EventId, Decision>>();
    let decisionsFromLatest = [] as Decision[];

    const self: Type<CType> = {
      register: (input) => {
        const {
          meta: { eventId },
          payload: { name },
        } = input;

        if (isWFCanonAdvrtMarker(input.payload)) {
          const map = advertisements.get(name) || new Map();
          if (!advertisements.has(name)) advertisements.set(name, map);
          map.set(eventId, input);
        } else if (isWFCanonDecideMarker(input.payload)) {
          const map = decisions.get(name) || new Map();
          if (!decisions.has(name)) decisions.set(name, map);
          map.set(eventId, input);

          decisionsFromLatest = sortByEventKey([
            ...decisionsFromLatest,
            input as Decision,
          ]).reverse();
        }
      },
      listDecisionsFromLatest: () => decisionsFromLatest,
      getAdvertisementsForName: (name) => {
        const ads = advertisements.get(name);
        if (!ads) return [];
        return sortByEventKey(Array.from(ads.values())).reverse();
      },
      getDecisionsForName: (name) => {
        const dec = decisions.get(name);
        if (!dec) return [];
        return sortByEventKey(Array.from(dec.values())).reverse();
      },
      getOpenAdvertisements: () =>
        Array.from(advertisements.entries())
          .filter(([name]) => !decisions.has(name))
          .flatMap(([_, ads]) => Array.from(ads.values())),
    };
    return self;
  };
}
