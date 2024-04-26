import { ActyxEvent } from "@actyx/sdk";
import { XEventKey } from "./ax-mock/index.js";
import { Ord } from "./utils.js";

export type CTypeProto = { ev: string; role: string };
export type MakeCType<CType extends CTypeProto> = CType;

/**
 * ActyxEvents that is Workflow-conformant will take this form
 */
export type WFEventOrDirective<CType extends CTypeProto> =
  | WFEvent<CType>
  | WFDirective;

/**
 * Events emitted by the actors explaining the interactions and state of the workflow
 */
export type WFEvent<CType extends CTypeProto> = {
  t: CType["ev"];
  payload: Record<string, unknown>;
};

/**
 * Events emitted by the intermediate machine to take note of the meta-state of the actor.
 * e.g. in a multiverse, in which universe the actor is in
 */
export type WFDirective =
  | {
      ax: InternalTag.StringOf<typeof InternalTag.CompensationNeeded>;
      actor: string;
      fromTimelineOf: string;
      toTimelineOf: string;
      compensationIndices: number[];
    }
  | {
      ax: InternalTag.StringOf<typeof InternalTag.CompensationDone>;
      actor: string;
      fromTimelineOf: string;
      toTimelineOf: string;
      compensationIndexDone: number;
    };

export type ActyxWFEvent<CType extends CTypeProto> = ActyxEvent<WFEvent<CType>>;
export type ActyxWFDirective = ActyxEvent<WFDirective>;
export type ActyxWFEventAndDirective<CType extends CTypeProto> = ActyxEvent<
  WFEventOrDirective<CType>
>;

export type Chain<CType extends CTypeProto> = ActyxWFEvent<CType>[];

/**
 * Reads as "A diverge from B at"
 * 0 means that the chain diverge at the first event
 * if result === A.length it means the chain doesn't diverge
 */
export const divertedFromOtherChainAt = <CType extends CTypeProto>(
  chainA: Chain<CType>,
  chainB: Chain<CType>
): number => {
  let sameIndex = 0;
  while (true) {
    const nextA = chainA.at(sameIndex);
    const nextB = chainB.at(sameIndex);
    if (!nextB) return chainA.length;
    if (!nextA) return sameIndex;
    if (nextA.meta.eventId !== nextB.meta.eventId) return sameIndex;
    sameIndex++;
  }
};

export const isWFEvent = <CType extends CTypeProto>(
  x: WFEventOrDirective<CType>
): x is WFEvent<CType> => "t" in x;

export const isWFDirective = <CType extends CTypeProto>(
  x: WFEventOrDirective<CType>
): x is WFDirective => "ax" in x;

export const extractWFEvents = <CType extends CTypeProto>(
  evs: ActyxWFEventAndDirective<CType>[]
): ActyxWFEvent<CType>[] =>
  evs.filter((ev): ev is ActyxWFEvent<CType> => isWFEvent(ev.payload));

export const extractWFDirective = <CType extends CTypeProto>(
  evs: ActyxWFEventAndDirective<CType>[]
): ActyxWFDirective[] =>
  evs.filter((ev): ev is ActyxWFDirective => isWFDirective(ev.payload));

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
}
