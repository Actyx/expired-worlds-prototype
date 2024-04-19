// Alan: Making this so that visualizing states of each actyx is easier
import { ActyxEvent } from "@actyx/sdk";
import { Obs } from "systemic-ts-utils";

export namespace Ord {
  export const Greater: unique symbol = Symbol("Greater");
  export const Lesser: unique symbol = Symbol("Lesser");
  export const Equal: unique symbol = Symbol("Equal");

  export type Type = typeof Greater | typeof Lesser | typeof Equal;

  export const OrdSym: unique symbol = Symbol("OrdTrait");
  export type OrdSym = typeof OrdSym;

  export type Cmp<T> = (a: T, b: T) => Type;
  export const cmp = <T extends { [OrdSym]: Cmp<T> }>(a: T, b: T) =>
    a[OrdSym](a, b);
}

export const Inner: unique symbol = Symbol("Inner");
export type Inner = typeof Inner;

export namespace Lamport {
  const ord: Ord.Cmp<Type> = (a, b) => {
    switch (true) {
      case a[Inner] > b[Inner]:
        return Ord.Greater;
      case a[Inner] < b[Inner]:
        return Ord.Lesser;
      default:
        return Ord.Equal;
    }
  };

  export type Type = {
    [Ord.OrdSym]: Ord.Cmp<Type>;
    [Inner]: number;
    clone: () => Type;
    incr: () => Type;
    max: (lamport: Type) => Type;
  };

  export const make = (x: number): Type => ({
    [Ord.OrdSym]: ord,
    [Inner]: x,
    clone: () => Lamport.make(x),
    incr: () => Lamport.make(x + 1),
    max: (lamport: Type) => Lamport.make(Math.max(lamport[Inner], x)),
  });
}

export namespace OwnStreamStore {
  export type Param = { lamport: Lamport.Type };
  export type Meta = { lamport: Lamport.Type; offset: number };
  export type Item = { meta: Meta; payload: unknown };

  export type Type = {
    push: ({ lamport }: Param, payload: unknown) => Meta;
    setAt: (offset: number, { lamport }: Param, payload: unknown) => Meta;
    stream: () => Stream.Type;
  };

  export const make = (): OwnStreamStore.Type => {
    const data = {
      data: [] as Item[],
    };

    return {
      push: ({ lamport }, payload) => {
        const offset = data.data.length;
        const meta: Meta = { lamport, offset };
        data.data.push({ meta, payload });
        return meta;
      },
      setAt: (offset, { lamport }, payload) => {
        const meta: Meta = { lamport, offset };
        data.data[offset] = { meta, payload };
        return meta;
      },
      stream: () => {
        let index = 0;
        return {
          index: () => index,
          next: () => {
            index = Math.max(index, data.data.length);
            return data.data.at(index) || null;
          },
          peek: () => data.data.at(index) || null,
        };
      },
    };
  };

  export namespace Stream {
    export type Type = {
      index: () => number;
      next: () => Item | null;
      peek: () => Item | null;
    };
  }
}

export namespace Node {
  export type Type = {
    subscribeMonotonic: (fn: (ev: ActyxEvent<unknown>) => unknown) => {
      kill: () => unknown;
    };
    in: (
      streamId: string,
      e: { meta: ActyxEvent["meta"]; payload: unknown }
    ) => unknown;
    out: Obs.Obs<ActyxEvent>;
  };

  export type Param = { id: string };

  export const make = (params: Param): Type => {
    const data = {
      local: OwnStreamStore.make(),
      remote: new Map() as Map<StreamId.StringRepr, OwnStreamStore.Type>,
      nextLamport: Lamport.make(0),
      out: Obs.Obs.make<ActyxEvent>(),
    };

    const getOrCreateStream = (streamId: string): OwnStreamStore.Type => {
      const item = data.remote.get(streamId) || OwnStreamStore.make();
      data.remote.set(streamId, item);
      return item;
    };

    return {
      subscribeMonotonic: (fn) => {},
      in: (streamId, e) => {
        data.nextLamport.max(Lamport.make(e.meta.lamport)).incr();
        getOrCreateStream(streamId).setAt(
          e.meta.offset,
          { lamport: Lamport.make(e.meta.lamport) },
          e.payload
        );
      },
      publish: (payload: unknown) => {
        const lamport = data.nextLamport;
        data.nextLamport = data.nextLamport.incr();
        const date = new Date();

        const meta = data.local.push({ lamport }, payload);
        const e: ActyxEvent = {
          meta: {
            ...meta,
            appId: "",
            eventId: "",
            isLocalEvent: true,
            lamport: meta.lamport[Inner],
            stream: params.id,
            tags: [],
            timestampAsDate: () => date,
            timestampMicros: date.getTime() * 1000,
          },
          payload,
        };

        data.out.emit(e);
      },
      out: data.out,
    };
  };
}

export namespace StreamId {
  export type StringRepr = string;
  export type Type = {
    [Inner]: ActyxEvent["meta"]["stream"];
  };
}
