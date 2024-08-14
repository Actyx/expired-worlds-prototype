import { ActyxEvent } from "@actyx/sdk";
import { XEventKey } from "./ax-mock/index.js";
import { Ord } from "./utils.js";

export type CTypeProto = { ev: string; role: string };
export type MakeCType<CType extends CTypeProto> = CType;

/**
 * ActyxEvents that is Workflow-conformant will take this form
 */
export type WFBusinessOrMarker<CType extends CTypeProto> =
  | WFBusiness<CType>
  | WFCanonMarker<CType>
  | WFCompensationMarker;

/**
 * Events emitted by the actors explaining the interactions and state of the workflow
 */
export type WFBusiness<CType extends CTypeProto> = {
  t: CType["ev"];
  payload: Record<string, unknown>;
};

export namespace NestedCodeIndexAddress {
  export type Type = number[];

  // example: [0] is smaller than [0,1]
  export const cmp = (a: Type, b: Type): Ord.Type => {
    const max = Math.max(a.length, b.length) - 1;
    let i = 0;
    while (i < max) {
      const itemA = a.at(i) || 0;
      const itemB = b.at(i) || 0;
      if (itemA < itemB) return Ord.Lesser;
      if (itemA > itemB) return Ord.Greater;
      i++;
    }
    return Ord.Equal;
  };
}

/**
 * Events emitted by the intermediate machine to take note of the meta-state of the actor.
 * e.g. in a multiverse, in which universe the actor is in
 */
export type WFCompensationMarker =
  | WFMarkerCompensationNeeded
  | WFMarkerCompensationDone;

export type WFCanonMarker<CType extends CTypeProto> =
  | WFMarkerCanonAdvrt<CType>
  | WFMarkerCanonDecide<CType>;

/**
 * Canonization advertisement is created by the emitter of the last event automatically.
 */
export type WFMarkerCanonAdvrt<CType extends CTypeProto> = {
  readonly ax: InternalTag.StringOf<typeof InternalTag.CanonAdvrt>;
  readonly name: CType["ev"];
  /**
   * Chain depth is how deep an event is from the root. `depth` works with
   * `name` to identify a canonization chance, enabling canonization inside a
   * loop.
   */
  readonly depth: number;
  /**
   * id of the advertiser
   */
  readonly advertiser: string;
  /**
   * id of the canonizer
   */
  readonly canonizer: string;
  // readonly codeIndex: NestedCodeIndexAddress.Type;
  readonly timelineOf: string;
};

export type WFMarkerCanonDecide<CType extends CTypeProto> = {
  readonly ax: InternalTag.StringOf<typeof InternalTag.CanonDecide>;
  readonly name: CType["ev"];
  /**
   * Chain depth is how deep an event is from the root. `depth` works with
   * `name` to identify a canonization chance, enabling canonization inside a
   * loop.
   */
  readonly depth: number;
  /**
   * id of the canonizer
   */
  readonly canonizer: string;
  // readonly codeIndex: NestedCodeIndexAddress.Type;
  readonly timelineOf: string;
};

export type WFMarkerCompensationNeeded = {
  readonly ax: InternalTag.StringOf<typeof InternalTag.CompensationNeeded>;
  readonly actor: string;
  /**
   * The first event of compensation, not the last one in digested event of a
   * particular actor Being the first event is important to be able to resolve
   * possible competing branches inside a compensation, including one that
   * happens during the switch between non-compensating to compensating
   */
  readonly fromTimelineOf: string;
  readonly toTimelineOf: string;
  /**
   * Supplemental information about which index the particular compensation code
   * exists in the workflow. IMPORTANT: a directive must be used in the same
   * context as the workflow it originated from.
   */
  readonly codeIndex: NestedCodeIndexAddress.Type;
};

export type WFMarkerCompensationDone = {
  readonly ax: InternalTag.StringOf<typeof InternalTag.CompensationDone>;
  readonly actor: string;
  /**
   * The first event of compensation, not the last one in digested event of a
   * particular actor Being the first event is important to be able to resolve
   * possible competing branches inside a compensation, including one that
   * happens during the switch between non-compensating to compensating
   */
  readonly fromTimelineOf: string;
  readonly toTimelineOf: string;
};

export type ActyxWFBusiness<CType extends CTypeProto> = ActyxEvent<
  WFBusiness<CType>
>;
export type ActyxWFCompensationMarker = ActyxEvent<WFCompensationMarker>;

export type ActyxWFCanonAdvrt<CType extends CTypeProto> = ActyxEvent<
  WFMarkerCanonAdvrt<CType>
>;

export type ActyxWFCanonDecide<CType extends CTypeProto> = ActyxEvent<
  WFMarkerCanonDecide<CType>
>;

export type ActyxWFCanonMarker<CType extends CTypeProto> =
  | ActyxWFCanonAdvrt<CType>
  | ActyxWFCanonDecide<CType>;

export type ActyxWFBusinessOrMarker<CType extends CTypeProto> = ActyxEvent<
  WFBusinessOrMarker<CType>
>;

// TODO: modify name, might mislead. Parallel is not part of the chain, because
// the history can actually be a graph, unlike for example, git
export type Chain<CType extends CTypeProto> = ActyxWFBusiness<CType>[];

export const sortByEventKey = <E>(chain: ActyxEvent<E>[]) => {
  if (chain.length <= 1) return chain;
  return chain
    .map((ev) => ({ ev, eventKey: XEventKey.fromMeta(ev.meta) }))
    .sort((a, b) => Ord.toNum(Ord.cmp(a.eventKey, b.eventKey)))
    .map((e) => e.ev);
};

/**
 * Last point where chainA and chainB has the same eventId
 */
export const divergencePoint = <CType extends CTypeProto>(
  chainA: Chain<CType>,
  chainB: Chain<CType>
): number => {
  let sameIndex = -1;
  while (true) {
    const a = chainA.at(sameIndex + 1);
    const b = chainB.at(sameIndex + 1);
    if (a === undefined && b === undefined) return sameIndex;
    if (a !== b) return sameIndex;
    sameIndex++;
  }
};

export const isWFEvent = <CType extends CTypeProto>(
  x: WFBusinessOrMarker<CType>
): x is WFBusiness<CType> => "t" in x;

export const isWFCompensationMarker = <CType extends CTypeProto>(
  x: WFBusinessOrMarker<CType>
): x is WFCompensationMarker =>
  "ax" in x &&
  (InternalTag.CompensationNeeded.is(x.ax) ||
    InternalTag.CompensationDone.is(x.ax));

export const isWFCanonDecideMarker = <CType extends CTypeProto>(
  x: WFBusinessOrMarker<CType>
): x is WFMarkerCanonDecide<CType> =>
  "ax" in x && InternalTag.CanonDecide.is(x.ax);

export const isWFCanonAdvrtMarker = <CType extends CTypeProto>(
  x: WFBusinessOrMarker<CType>
): x is WFMarkerCanonAdvrt<CType> =>
  "ax" in x && InternalTag.CanonAdvrt.is(x.ax);

const genExtractWithPayloadCondition = <
  CType extends CTypeProto,
  T extends WFBusinessOrMarker<CType>
>(
  condition: (x: WFBusinessOrMarker<CType>) => x is T
) => {
  return (evs: ActyxWFBusinessOrMarker<CType>[]): ActyxEvent<T>[] =>
    evs.filter((ev): ev is ActyxEvent<T> => condition(ev.payload));
};

export const extractWFBusinessEvents = <CType extends CTypeProto>(
  evs: ActyxWFBusinessOrMarker<CType>[]
): ActyxWFBusiness<CType>[] =>
  evs.filter((ev): ev is ActyxWFBusiness<CType> => isWFEvent(ev.payload));

export const extractWFCompensationMarker = <CType extends CTypeProto>(
  evs: ActyxWFBusinessOrMarker<CType>[]
): ActyxWFCompensationMarker[] =>
  evs.filter((ev): ev is ActyxWFCompensationMarker =>
    isWFCompensationMarker(ev.payload)
  );

export const extractWFCanonMarker = <CType extends CTypeProto>(
  evs: ActyxWFBusinessOrMarker<CType>[]
): ActyxWFCanonMarker<CType>[] =>
  evs.filter(
    (ev): ev is ActyxWFCanonMarker<CType> =>
      isWFCanonDecideMarker(ev.payload) || isWFCanonAdvrtMarker(ev.payload)
  );

export const extractWFCanonDecideMarker = genExtractWithPayloadCondition(
  isWFCanonDecideMarker
);

export const extractWFCanonAdvrtMarker =
  genExtractWithPayloadCondition(isWFCanonAdvrtMarker);

export namespace InternalTag {
  /**
   * The form of InternalTag.Tool
   */
  export type Tool<T extends string> = {
    write: (s: string) => String<T>;
    is: (s: string) => s is String<T>;
    read: (s: string) => string | null;
  };

  // Typed substring representation

  type String<T extends string> = `${T}${string}`;
  export type StringOf<T extends Tool<any>> = T extends Tool<infer X>
    ? `${X}${string}`
    : never;

  const factory = <T extends string>(prefix: T): Tool<T> => {
    const write = (id: string): String<T> => `${prefix}${id}`;
    const is = (s: string): s is String<T> => s.startsWith(prefix);
    const read = (s: string) => {
      if (s.startsWith(prefix)) {
        return s.slice(prefix.length);
      }
      return null;
    };
    return { write, read, is };
  };

  /**
   * Marks the writer of the event
   */
  export const ActorWriter = factory("ax:wf:by:");

  /**
   * For marking events that are part of compensation
   */
  export const CompensationEvent = factory("ax:wf:compensation:event:");
  /**
   * For marking predecessor eventId, placed inside a tag
   */
  export const Predecessor = factory("ax:wf:predecessor:");
  /**
   * For marking that an actor needs to compensate for something
   */
  export const CompensationNeeded = factory("ax:wf:compensation:needed:");
  /**
   * For marking that an actor doesn't need to do compensation anymore
   */
  export const CompensationDone = factory("ax:wf:compensation:done:");

  /**
   * For marking that an event is canon.
   */
  export const CanonDecide = factory("ax:wf:canon:decide:");
  export const CanonAdvrt = factory("ax:wf:canon:req:");
}
