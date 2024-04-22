// Alan: Making this so that visualizing states of each actyx is easier
import { ActyxEvent, Actyx, OffsetMap, Offset } from "@actyx/sdk";
import { Obs } from "systemic-ts-utils";
import * as uuid from "uuid";

type NodeId = string;
type PartitionId = string;
type MiniOffsetMap = Record<string, number>;

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

export namespace StreamStore {
  export type Param = { lamport: Lamport.Type };

  export type Type = Readonly<{
    offset: () => number;
    set: (e: ActyxEvent) => void;
    stream: () => Stream.Type;
    slice: ActyxEvent[]["slice"];
  }>;

  export const make = (): StreamStore.Type => {
    const data = {
      data: [] as ActyxEvent[],
    };

    return {
      offset: () => data.data.length,
      set: (e) => {
        data.data[e.meta.offset] = e;
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
      slice: (...args) => data.data.slice(...args),
    };
  };

  export namespace Stream {
    export type Type = {
      index: () => number;
      next: () => ActyxEvent | null;
      peek: () => ActyxEvent | null;
    };
  }
}

export namespace Node {
  export type Answer = {
    from: NodeId;
    evs: ActyxEvent[];
  };

  export type Ask = {
    from: NodeId;
    offsetMap: MiniOffsetMap;
  };

  export type Type = Readonly<{
    id: Readonly<string>;
    api: {
      stores: () => {
        own: StreamStore.Type;
        remote: Map<string, StreamStore.Type>;
      };
      // subscribeMonotonic: (fn: (ev: ActyxEvent<unknown>) => unknown) => {
      //   kill: () => unknown;
      // };
      publish: (e: unknown) => void;
      offsetMap: () => MiniOffsetMap;
    };
    coord: {
      connected: () => unknown;
      in: (e: { meta: ActyxEvent["meta"]; payload: unknown }) => unknown;
      out: Obs.Obs<ActyxEvent>;
      ask: Obs.Obs<Ask>;
      receiveAsk: (ask: Ask) => void;
      answer: Obs.Obs<Answer>;
      receiveAnswer: (answer: Answer) => void;
    };
  }>;

  export type Param = { id: string };

  export const make = (params: Param): Type => {
    const data = {
      own: StreamStore.make(),
      remote: new Map() as Map<string, StreamStore.Type>,
      nextLamport: Lamport.make(0),
    };

    const offsetMap = (): MiniOffsetMap => {
      const map = {} as MiniOffsetMap;
      Array.from(data.remote.entries()).forEach(([nodeId, stream]) => {
        if (stream.offset() > 0) {
          map[nodeId] = stream.offset();
        }
      });
      if (data.own.offset() > 0) {
        map[params.id] = data.own.offset();
      }
      return map;
    };

    const getOrCreateStream = (streamId: string): StreamStore.Type => {
      const item = data.remote.get(streamId) || StreamStore.make();
      data.remote.set(streamId, item);
      return item;
    };

    const api: Type["api"] = {
      offsetMap,
      stores: () => ({
        own: data.own,
        remote: new Map(data.remote),
      }),
      publish: (payload: unknown) => {
        const lamport = data.nextLamport;
        const date = new Date();

        const e: ActyxEvent = {
          meta: {
            offset: data.own.offset(),
            appId: "",
            eventId: uuid.v4(),
            isLocalEvent: true,
            lamport: lamport[Inner],
            stream: params.id,
            tags: [],
            timestampAsDate: () => date,
            timestampMicros: date.getTime() * 1000,
          },
          payload,
        };
        data.own.set(e);
        data.nextLamport = data.nextLamport.incr();

        coord.out.emit(e);
      },
    };

    const coord: Type["coord"] = {
      out: Obs.Obs.make(),
      in: (e) => {
        const { stream } = e.meta;
        if (stream === params.id) return;
        data.nextLamport.max(Lamport.make(e.meta.lamport)).incr();
        getOrCreateStream(stream).set(e);
      },
      connected: () =>
        coord.ask.emit({
          from: params.id,
          offsetMap: offsetMap(),
        }),
      ask: Obs.Obs.make(),
      receiveAsk: (ask) => {
        const selfOffset = ask.offsetMap[params.id] || 0;
        const answer = data.own.slice(selfOffset);
        if (answer.length === 0) return;
        coord.answer.emit({ from: params.id, evs: answer });
      },
      answer: Obs.Obs.make(),
      receiveAnswer: (answer) => {
        const streamStore = getOrCreateStream(answer.from);
        answer.evs.forEach((ev) => streamStore.set(ev));
      },
    };

    return {
      id: params.id,
      api,
      coord,
    };
  };
}

export namespace Network {
  export type Type = Readonly<{
    join: (_: Node.Type) => Promise<void>;
    partitions: {
      make: (ids: Node.Type[]) => void;
      clear: () => Promise<void>;
    };
  }>;

  export const make = (): Type => {
    const data = {
      nodes: new Map() as Map<string, Node.Type>,
      partitions: {
        forward: new Map<NodeId, { partitionId: string }>(),
        reverse: new Map<PartitionId, { nodes: NodeId[] }>(),
      },
    };

    const getNeighbors = (selfId: string): Node.Type[] => {
      if (!data.nodes.has(selfId)) return [];
      const inPartition = data.partitions.forward.get(selfId) || null;

      if (inPartition === null) {
        const neighbors = Array.from(data.nodes)
          .filter(
            ([id, _]) => !data.partitions.forward.has(id) && id !== selfId
          )
          .map(([_, node]) => node);
        return neighbors;
      }
      const neighbors = (
        data.partitions.reverse.get(inPartition.partitionId)?.nodes || []
      )
        .map((x) => data.nodes.get(x) || null)
        .filter(
          (node): node is Node.Type => node !== null && node.id !== selfId
        );
      return neighbors;
    };

    const res: Type = {
      join: (node) => {
        data.nodes.set(node.id, node);
        node.coord.out.sub((e) =>
          getNeighbors(e.meta.stream).map((node) => node.coord.in(e))
        );
        node.coord.ask.sub((ask) =>
          getNeighbors(ask.from).forEach((node) => node.coord.receiveAsk(ask))
        );
        node.coord.answer.sub((answer: Node.Answer) =>
          getNeighbors(answer.from).forEach((node) =>
            node.coord.receiveAnswer(answer)
          )
        );

        return new Promise((res) =>
          setImmediate(() => {
            getNeighbors(node.id).forEach((node) => node.coord.connected());
            node.coord.connected();
            res();
          })
        );
      },
      partitions: {
        make: (nodes) => {
          const partitionId = uuid.v4();
          const ids = nodes.map((x) => x.id).filter((x) => data.nodes.has(x));
          ids.forEach((x) => data.partitions.forward.set(x, { partitionId }));
          data.partitions.reverse.set(partitionId, { nodes: [...ids] });
        },
        clear: () => {
          data.partitions.forward = new Map();
          data.partitions.reverse = new Map();

          return new Promise((res) =>
            setImmediate(() => {
              data.nodes.forEach((node) => node.coord.connected());
              res();
            })
          );
        },
      },
    };

    return res;
  };
}
