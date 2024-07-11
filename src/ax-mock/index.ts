// Alan: Making this so that visualizing states of each actyx is easier
import {
  ActyxEvent,
  MsgType,
  OnCompleteOrErr,
  CancelSubscription,
  EventsOrTimetravel,
  TaggedEvent,
} from "@actyx/sdk";
import { Obs } from "systemic-ts-utils";
import * as uuid from "uuid";
import { Logger, makeLogger, Ord } from "../utils.js";

type NodeId = string;
type PartitionId = string;
type MiniOffsetMap = Record<string, number>;

export const Inner: unique symbol = Symbol("Inner");
export type Inner = typeof Inner;

export namespace XLamport {
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
  };

  export const make = (x: number): Type => ({
    [Ord.OrdSym]: ord,
    [Inner]: x,
    clone: () => XLamport.make(x),
    incr: () => XLamport.make(x + 1),
  });

  export const max = (a: Type, b: Type) =>
    XLamport.make(Math.max(a[Inner], b[Inner]));
}

export namespace XEventKey {
  const ord: Ord.Cmp<Type> = (a, b) => {
    switch (Ord.cmp(a[Inner].lamport, b[Inner].lamport)) {
      case Ord.Greater:
        return Ord.Greater;
      case Ord.Lesser:
        return Ord.Lesser;
      default:
        return Ord.ofString(a[Inner].streamId, b[Inner].streamId);
    }
  };

  export type Type = {
    [Ord.OrdSym]: Ord.Cmp<Type>;
    [Inner]: { lamport: XLamport.Type; streamId: string };
  };

  export const fromMeta = (meta: ActyxEvent["meta"]) =>
    make(XLamport.make(meta.lamport), meta.stream);

  export const make = (lamport: XLamport.Type, streamId: string) => ({
    [Ord.OrdSym]: ord,
    [Inner]: {
      lamport,
      streamId,
    },
  });
}

export namespace StreamStore {
  export type Param = { lamport: XLamport.Type };

  export type Type<E> = Readonly<{
    offset: () => number;
    set: (e: ActyxEvent<E>) => void;
    stream: () => Stream.Type<E>;
    slice: ActyxEvent<E>[]["slice"];
  }>;

  export const make = <E>(): StreamStore.Type<E> => {
    const data = {
      data: [] as ActyxEvent<E>[],
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
            const item = data.data.at(index) || null;
            index = Math.min(index + 1, data.data.length);
            return item;
          },
          peek: () => data.data.at(index) || null,
        };
      },
      slice: (...args) => data.data.slice(...args),
    };
  };

  export namespace Stream {
    export type Type<E> = {
      index: () => number;
      next: () => ActyxEvent<E> | null;
      peek: () => ActyxEvent<E> | null;
    };
  }
}

export namespace Node {
  export type Answer<E> = {
    from: NodeId;
    evs: ActyxEvent<E>[];
  };

  export type Ask = {
    from: NodeId;
    offsetMap: MiniOffsetMap;
  };

  export type Type<E> = Readonly<{
    id: Readonly<string>;
    api: {
      stores: () => {
        own: StreamStore.Type<E>;
        remote: Map<string, StreamStore.Type<E>>;
      };
      subscribeMonotonic: (
        callback: (data: EventsOrTimetravel<E>) => Promise<void> | void,
        onCompleteOrErr?: OnCompleteOrErr
      ) => CancelSubscription;
      publish: (e: TaggedEvent) => void;
      offsetMap: () => MiniOffsetMap;
    };
    coord: {
      startSync: () => unknown;
      in: (e: ActyxEvent<E>) => unknown;
      out: Obs.Obs<ActyxEvent<E>>;

      ask: Obs.Obs<Ask>;
      recAsk: (ask: Ask) => void;
      answer: Obs.Obs<Answer<E>>;
      recAnswer: (answer: Answer<E>) => void;

      afterSync: () => void;
    };
    store: {
      save: () => ActyxEvent<E>[];
      load: (evs: ActyxEvent<E>[]) => void;
    };
    logger: Logger;
  }>;

  export type Param = { id: string };

  export const make = <E>(params: Param): Type<E> => {
    const logger = makeLogger(`axmock:${params.id}`);
    const data = {
      own: StreamStore.make<E>(),
      remote: new Map() as Map<string, StreamStore.Type<E>>,
      nextLamport: XLamport.make(0),
      inToSubPipe: Obs.Obs.make<ActyxEvent<E>>(),
      timeTravelAlert: Obs.Obs.make<void>(),
      afterSync: Obs.Obs.make<void>(),
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

    const getOrCreateStream = (streamId: string): StreamStore.Type<E> => {
      const item = data.remote.get(streamId) || StreamStore.make();
      data.remote.set(streamId, item);
      return item;
    };

    const mergedOrdered = (inputStreams: StreamStore.Stream.Type<E>[]) => {
      const streams = new Set(inputStreams);

      const findNext = () => {
        const markedEmpty = new Set<StreamStore.Stream.Type<E>>();
        let pairOrNull = null as
          | null
          | [StreamStore.Stream.Type<E>, ActyxEvent<E>];

        streams.forEach((stream) => {
          const item = stream.peek();
          if (item === null) {
            markedEmpty.add(stream);
            return;
          }

          if (!pairOrNull) {
            pairOrNull = [stream, item];
          } else {
            const [_, oldItem] = pairOrNull;
            if (
              Ord.cmp(
                XEventKey.fromMeta(item.meta),
                XEventKey.fromMeta(oldItem.meta)
              ) === Ord.Lesser
            ) {
              pairOrNull = [stream, item];
            }
          }
        }, null);
        markedEmpty.forEach((stream) => streams.delete(stream));
        if (!pairOrNull) return null;

        const [stream, item] = pairOrNull;
        stream.next();
        return item;
      };

      return {
        next: findNext,
      };
    };

    const api: Type<E>["api"] = {
      offsetMap,
      stores: () => ({
        own: data.own,
        remote: new Map(data.remote),
      }),
      subscribeMonotonic: (handler, onCompleteOrErr) => {
        let alive = true;
        const wrappedHandler: typeof handler = (...args) => {
          try {
            handler(...args);
          } catch (error) {
            console.log(error);
          }
        };
        const unsubs = [] as (() => unknown)[];

        // deferred streaming

        const setupStream = () => {
          const stopAndCleanup = () => {
            unsubs.forEach((x) => x());
            unsubs.length = 0;
          };

          const restartStream = () => {
            setupStream();
          };

          const stream = mergedOrdered([
            data.own.stream(),
            ...Array.from(data.remote.values()).map((store) => store.stream()),
          ]);
          let bounded = [] as ActyxEvent<E>[];

          while (alive) {
            const next = stream.next();
            if (!next) break;
            bounded.push(next);
          }
          if (!alive) return;
          wrappedHandler({
            type: MsgType.events,
            caughtUp: true,
            events: bounded,
          });

          unsubs.push(
            coord.out.sub((x) =>
              wrappedHandler({
                type: MsgType.events,
                caughtUp: true,
                events: [x],
              })
            ),
            data.inToSubPipe.sub((x) =>
              wrappedHandler({
                type: MsgType.events,
                caughtUp: true,
                events: [x],
              })
            ),
            data.timeTravelAlert.sub(() => {
              stopAndCleanup();
              const afterAsk = () => {
                restartStream();
                data.afterSync.unsub(afterAsk);
              };
              data.afterSync.sub(afterAsk);
              wrappedHandler({
                type: MsgType.timetravel,
                trigger: {} as any, // TODO: fix
              });
            })
          );
        };

        setImmediate(setupStream);

        return () => {
          alive = false;
          unsubs.forEach((x) => x());
          onCompleteOrErr?.();
        };
      },
      publish: (tagged: TaggedEvent) => {
        const lamport = data.nextLamport;
        const date = new Date();

        const e: ActyxEvent<E> = {
          meta: {
            offset: data.own.offset(),
            appId: "",
            eventId: uuid.v4(),
            isLocalEvent: true,
            lamport: lamport[Inner],
            stream: params.id,
            tags: tagged.tags,
            timestampAsDate: () => date,
            timestampMicros: date.getTime() * 1000,
          },
          payload: tagged.event as E,
        };
        data.own.set(e);
        data.nextLamport = data.nextLamport.incr();

        coord.out.emit(e);
      },
    };

    const coord: Type<E>["coord"] = {
      out: Obs.Obs.make(),
      in: (e) => {
        const { stream } = e.meta;
        if (stream === params.id) return;
        data.nextLamport = XLamport.max(
          data.nextLamport,
          XLamport.make(e.meta.lamport).incr()
        );
        getOrCreateStream(stream).set(e);
        data.inToSubPipe.emit(e);
      },
      startSync: () => {
        coord.ask.emit({
          from: params.id,
          offsetMap: offsetMap(),
        });
      },
      ask: Obs.Obs.make(),
      recAsk: (ask) => {
        const selfOffset = ask.offsetMap[params.id] || 0;
        const answer = data.own.slice(selfOffset);
        if (answer.length === 0) return;
        coord.answer.emit({ from: params.id, evs: answer });
      },
      answer: Obs.Obs.make(),
      recAnswer: (answer) => {
        const streamStore = getOrCreateStream(answer.from);
        let timetravel = false;

        answer.evs.forEach((e) => {
          streamStore.set(e);
          const evLamport = XLamport.make(e.meta.lamport);

          const ord = Ord.cmp(evLamport, data.nextLamport);
          if (ord === Ord.Lesser || ord === Ord.Equal) {
            timetravel = true;
          }

          data.nextLamport = XLamport.max(data.nextLamport, evLamport.incr());
        });

        if (timetravel) {
          data.timeTravelAlert.emit();
        }
      },
      afterSync: () => {
        data.afterSync.emit();
      },
    };

    const store: Type<E>["store"] = {
      save: () => data.own.slice(),
      load: (evs) => {
        evs
          .filter((e) => e.meta.stream === params.id)
          .forEach((e) => {
            data.own.set(e);
            data.nextLamport = XLamport.max(
              data.nextLamport,
              XLamport.make(e.meta.lamport).incr()
            );
          });
      },
    };

    return {
      logger,
      id: params.id,
      api,
      coord,
      store,
    };
  };
}

export namespace Network {
  export type Type<E> = Readonly<{
    logger: Logger;
    join: (_: Node.Type<E>) => Promise<void>;
    partitions: {
      make: (nodes: Node.Type<E>[]) => void;
      clear: () => Promise<void>;
    };
  }>;

  export const make = <E>(): Type<E> => {
    const logger = makeLogger(`network:${uuid.v4()}`);

    const data = {
      nodes: new Map() as Map<string, Node.Type<E>>,
      partitions: {
        forward: new Map<NodeId, { partitionId: string }>(),
        reverse: new Map<PartitionId, { nodes: NodeId[] }>(),
      },
    };

    const getNeighbors = (selfId: string): Node.Type<E>[] => {
      if (!data.nodes.has(selfId)) return [];
      const inPartition = data.partitions.forward.get(selfId) || null;

      if (inPartition === null) {
        // find everyone that's not in any non-general partition
        const neighbors = Array.from(data.nodes)
          .map(([_, node]) => node)
          .filter(
            (node) =>
              !data.partitions.forward.has(node.id) && node.id !== selfId
          );
        return neighbors;
      }

      const nodesInPartition =
        data.partitions.reverse.get(inPartition.partitionId)?.nodes || [];

      const neighbors = nodesInPartition
        .map((x) => data.nodes.get(x) || null)
        .filter(
          (node): node is Node.Type<E> => node !== null && node.id !== selfId
        );
      return neighbors;
    };

    const res: Type<E> = {
      logger,
      join: (node) => {
        data.nodes.set(node.id, node);
        node.coord.out.sub((e) =>
          getNeighbors(e.meta.stream).map((node) => node.coord.in(e))
        );
        node.coord.ask.sub((ask) =>
          getNeighbors(ask.from).forEach((node) => node.coord.recAsk(ask))
        );
        node.coord.answer.sub((answer: Node.Answer<E>) =>
          getNeighbors(answer.from).forEach((node) =>
            node.coord.recAnswer(answer)
          )
        );

        return new Promise((res) =>
          setImmediate(() => {
            const nodes = [...getNeighbors(node.id), node];
            nodes.forEach((node) => node.coord.startSync());
            nodes.forEach((node) => node.coord.afterSync());

            res();
          })
        );
      },
      partitions: {
        make: (nodes) => {
          const nodeIds = nodes
            .map((x) => x.id)
            .filter((x) => data.nodes.has(x));

          // remove from old partition if applies
          nodeIds.forEach((idRemovable) => {
            const inPartition = data.partitions.forward.get(idRemovable);
            if (!inPartition) return;
            const partition = data.partitions.reverse.get(
              inPartition.partitionId
            );
            if (!partition) return;
            partition.nodes = partition.nodes.filter(
              (id) => id !== idRemovable
            );
          });

          // add to new partition
          const partitionId = uuid.v4();
          nodeIds.forEach((x) =>
            data.partitions.forward.set(x, { partitionId })
          );
          data.partitions.reverse.set(partitionId, { nodes: [...nodeIds] });
        },
        clear: () => {
          data.partitions.forward = new Map();
          data.partitions.reverse = new Map();

          return new Promise((res) =>
            setImmediate(() => {
              data.nodes.forEach((node) => node.coord.startSync());
              data.nodes.forEach((node) => node.coord.afterSync());

              res();
            })
          );
        },
      },
    };

    return res;
  };
}
