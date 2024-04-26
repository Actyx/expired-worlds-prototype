import { XEventKey } from "./ax-mock/index.js";
import { ActyxWFEvent, CTypeProto, InternalTag } from "./consts.js";
import { ExcludeArrayMember, NumArrToCodepoint, Ord } from "./utils.js";

type EventId = string;
type EventInfo<CType extends CTypeProto> = ActyxWFEvent<CType>;

export namespace MultiverseTree {
  export const UnregisteredEvent: unique symbol = Symbol("UnregisteredEvent");
  export type UnregisteredEvent = typeof UnregisteredEvent;
  export type Type<CType extends CTypeProto> = {
    register: (e: ActyxWFEvent<CType>) => void;
    getPredecessor: (
      e: ActyxWFEvent<CType>
    ) => ActyxWFEvent<CType> | UnregisteredEvent | null;
    has: (e: ActyxWFEvent<CType>) => boolean;
    isHead: (e: ActyxWFEvent<CType>) => boolean;
    isRoot: (e: ActyxWFEvent<CType>) => boolean;
    isCanon: (e: ActyxWFEvent<CType>) => boolean;
    /**
     * Get a chain of events, both backward and forward.
     * On the forward case, it goes to the future until at the point where event branches
     */
    getChainBackwards: (e: ActyxWFEvent<CType>) => null | ActyxWFEvent<CType>[];
    getChainBidirectionally: (
      e: ActyxWFEvent<CType>
    ) => null | ActyxWFEvent<CType>[];
    getCanonChain: () => null | ActyxWFEvent<CType>[];
  };

  type RootInfo = { canonHead: EventId; canonChain: EventId[] };

  export const make = <CType extends CTypeProto>(): Type<CType> => {
    const internal = {
      infoMap: new Map<EventId, EventInfo<CType>>(),
      predecessors: new Map<EventId, EventId>(),
      next: new Map<EventId, Set<EventId>>(),
      rootOf: new Map<EventId, RootInfo>(),
      canonHeadOf: new Map<EventId, { root: EventId }>(), // EventId1 is the head of EventId2 root
    };

    const recalculateCanonChain = (eventId: string): RootInfo => {
      const chain = [eventId];
      while (true) {
        const next = Array.from(internal.next.get(chain[0]) || [])
          .map((eventId) => {
            const event = internal.infoMap.get(eventId);
            if (!event) return null;
            return { eventId, event, key: XEventKey.fromMeta(event.meta) };
          })
          .filter((x): x is Exclude<typeof x, null> => x !== null)
          .sort((a, b) => Ord.toNum(Ord.cmp(a.key, b.key)))
          .at(0);

        if (!next) {
          return {
            canonHead: chain[chain.length - 1],
            canonChain: chain,
          };
        }

        chain.push(next.eventId);
      }
    };

    const findRootData = (eventId: string): null | [EventId, RootInfo] => {
      let pointer = eventId;

      while (true) {
        const rootInfo = internal.rootOf.get(pointer);
        if (rootInfo) return [pointer, rootInfo];

        const rootThroughCanonHeadOf = internal.canonHeadOf.get(pointer);
        if (rootThroughCanonHeadOf) {
          pointer = rootThroughCanonHeadOf.root;
          continue;
        }

        const predecessor = internal.predecessors.get(pointer);
        if (predecessor !== undefined) {
          pointer = predecessor;
          continue;
        }

        return null;
      }
    };

    /**
     * populate the past up until root
     */
    const populateChainBackwards = (chain: string[]) => {
      while (true) {
        const predecessorEventId = internal.predecessors.get(chain[0]) || null;
        if (predecessorEventId === null) break;
        chain.unshift(predecessorEventId);
      }
    };

    /**
     * populate until certain future (next.events === 1)
     */
    const populateChainForwardCertain = (chain: string[]) => {
      while (true) {
        const nextEvents = internal.next.get(chain[chain.length - 1]);
        if (nextEvents?.size !== 1) break;
        const nextEvent = nextEvents.values().next().value as string;
        chain.push(nextEvent);
      }
    };

    const self: Type<CType> = {
      register: (ev) => {
        const { eventId } = ev.meta;
        if (internal.predecessors.has(eventId)) return;

        internal.infoMap.set(eventId, ev);
        const predecessorId = ev.meta.tags.find((x) =>
          InternalTag.Predecessor.read(x)
        );
        if (!predecessorId) {
          internal.rootOf.set(eventId, {
            canonHead: eventId,
            canonChain: [eventId],
          });
          internal.canonHeadOf.set(eventId, { root: eventId });
        } else {
          internal.predecessors.set(eventId, predecessorId);

          const nextSet = internal.next.get(predecessorId) || new Set();
          nextSet.add(eventId);
          internal.next.set(predecessorId, nextSet);

          // head calculation
          const root = findRootData(predecessorId);
          if (root) {
            const [rootEventId, rootData] = root;
            internal.canonHeadOf.delete(rootData.canonHead);
            if (predecessorId === rootData.canonHead) {
              rootData.canonHead = eventId;
              rootData.canonChain.push(eventId);
              internal.canonHeadOf.set(eventId, { root: rootEventId });
            } else {
              const { canonHead: head, canonChain: chain } =
                recalculateCanonChain(rootEventId);
              rootData.canonHead = head;
              rootData.canonChain = chain;
              internal.canonHeadOf.set(head, { root: rootEventId });
            }
          } else {
            // if root not registered, skip root and chain analysis
          }
        }
      },
      has: (ev) => internal.infoMap.has(ev.meta.eventId),
      getPredecessor: (ev) => {
        const predecessorId = internal.predecessors.get(ev.meta.eventId);
        if (predecessorId === undefined) return null;
        const info = internal.infoMap.get(predecessorId);
        if (info === undefined) return UnregisteredEvent;
        return info;
      },
      isHead: (ev) => internal.canonHeadOf.has(ev.meta.eventId),
      isCanon: (ev) => {
        // Note: non-optimal
        return (
          Array.from(internal.rootOf.values()).findIndex((root) => {
            return (
              root.canonChain.findIndex(
                (eventIdInChain) => ev.meta.eventId === eventIdInChain
              ) !== -1
            );
          }) !== -1
        );
      },
      isRoot: (ev) => internal.rootOf.has(ev.meta.eventId),
      getChainBackwards: (ev) => {
        const idChain = [ev.meta.eventId]; // chronological event id chains
        populateChainBackwards(idChain);
        const infoChain = idChain.map((x) => internal.infoMap.get(x) || null);
        // get rid of incomplete chain due to malformed query or deleted events
        if (infoChain.findIndex((x) => x === null) !== -1) return null;

        return infoChain as ExcludeArrayMember<typeof infoChain, null>;
      },
      getChainBidirectionally: (ev) => {
        const idChain = [ev.meta.eventId]; // chronological event id chains
        populateChainBackwards(idChain);
        populateChainForwardCertain(idChain);
        const infoChain = idChain.map((x) => internal.infoMap.get(x) || null);
        // get rid of incomplete chain due to malformed query or deleted events
        if (infoChain.findIndex((x) => x === null) !== -1) return null;

        return infoChain as ExcludeArrayMember<typeof infoChain, null>;
      },
      getCanonChain: () => {
        const canonRoot = Array.from(internal.rootOf.entries())
          .map(([eventId, rootData]) => {
            const rootEv = internal.infoMap.get(eventId);
            if (!rootEv) return null;
            return { rootEv, eventId, rootData };
          })
          .filter((x): x is Exclude<typeof x, null> => x !== null)
          .sort((a, b) =>
            Ord.toNum(
              Ord.cmp(
                XEventKey.fromMeta(a.rootEv.meta),
                XEventKey.fromMeta(b.rootEv.meta)
              )
            )
          )
          .at(0);

        if (!canonRoot) return null;

        const infoChain = canonRoot.rootData.canonChain.map(
          (x) => internal.infoMap.get(x) || null
        );
        if (infoChain.findIndex((x) => x === null) !== -1) return null;
        return infoChain as ExcludeArrayMember<typeof infoChain, null>;
      },
    };

    return self;
  };
}
