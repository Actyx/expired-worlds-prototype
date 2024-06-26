import { ActyxWFBusiness, CTypeProto, InternalTag } from "./consts.js";

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
