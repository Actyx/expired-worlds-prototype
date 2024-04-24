import { ActyxEvent } from "@actyx/sdk";

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
export type WFDirective = {
  ax:
    | InternalTag.StringOf<typeof InternalTag.CompensationNeeded>
    | InternalTag.StringOf<typeof InternalTag.CompensationFinished>;
  actor: string;
  at: string;
};

export type ActyxWFEvent<CType extends CTypeProto> = ActyxEvent<WFEvent<CType>>;
export type ActyxWFDirective = ActyxEvent<WFDirective>;
export type ActyxWFEventAndDirective<CType extends CTypeProto> = ActyxEvent<
  WFEventOrDirective<CType>
>;

export const isWFEvent = <CType extends CTypeProto>(
  x: WFEventOrDirective<CType>
): x is WFEvent<CType> => "t" in x;

export const extractWFEvents = <CType extends CTypeProto>(
  evs: ActyxWFEventAndDirective<CType>[]
): ActyxWFEvent<CType>[] =>
  evs.filter((ev): ev is ActyxWFEvent<CType> => isWFEvent(ev.payload));

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
    ? X
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
  export const CompensationNeeded = factory("ax:wf:compensation:start:");
  /**
   * For marking that an actor doesn't need to do compensation anymore
   */
  export const CompensationFinished = factory("ax:wf:compensation:finished:");
}
