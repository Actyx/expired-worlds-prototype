import {
  Actyx,
  ActyxEvent,
  CancelSubscription,
  EventsOrTimetravel,
  MonotonicSubscription,
  MsgType,
  OnCompleteOrErr,
} from "@actyx/sdk";

export type Params<E> = {
  actyx: Parameters<(typeof Actyx)["of"]>;
  query: MonotonicSubscription<E>;
};
type OnEventsOrTimetravel<E> = (data: EventsOrTimetravel<E>) => Promise<void>;

export type Actuality<E> = {
  self: () => E;
  previous: () => Actuality<E>;
  root: () => Actuality<E>;
  expired: () => boolean;
};

export namespace Actuality {
  export const Snapshot: unique symbol = Symbol("Snapshot");
  export type Snapshot = typeof Snapshot;

  export const Actual: unique symbol = Symbol("Actual");
  export type Actual = typeof Actual;

  export type Actuality<E> = {
    t: Actual;
    self: () => ActyxEvent<E>;
    previous: () => null | ActualitySnapshot<E>;
    root: () => ActualitySnapshot<E>;
    canon: () => boolean;
    expired: () => boolean;
  };

  export type ActualitySnapshot<E> = {
    t: Snapshot;
    self: () => ActyxEvent<E>;
    previous: () => null | ActualitySnapshot<E>;
    root: () => ActualitySnapshot<E>;
    canon: () => boolean;
    expired: () => boolean;
  };

  type Tracker<E> = {
    push: (e: ActyxEvent<E>) => void;
    expire: () => void;
    getActuality: () => Promise<Actuality<E>>;
  };

  type Canon<E> = {
    root: ActyxEvent<E>;
    map: Map<ActyxEvent<E>["meta"]["eventId"], ActyxEvent<E>>;
    chain: ActyxEvent<E>[];
  };

  type TrackerInternal<E> = {
    readonly globalEventMap: Map<
      ActyxEvent<E>["meta"]["eventId"],
      ActyxEvent<E>
    >;
    canon: null | Canon<E>;
    expired: Canon<E>[];
    actualityCache:
      | { resolve: (item: Canon<E>) => void; reject: () => void }[]
      | Actuality<E>;
  };

  type ActualityParam<E> = {
    canon: Canon<E>;
    isCanon: () => boolean;
  };

  const previous = <E>(param: ActualityParam<E>, actyxEvent: ActyxEvent<E>) => {
    const { canon } = param;
    const index = canon.chain.indexOf(actyxEvent);
    if (actyxEvent === canon.root) return null; // optimization - root cannot have previous
    const previousActyxEvent = canon.chain.at(index + 1);
    if (!previousActyxEvent) return null;
    return snapshot(param, previousActyxEvent);
  };

  const root = <E>(param: ActualityParam<E>) =>
    snapshot(param, param.canon.root);

  const snapshot = <E>(
    param: ActualityParam<E>,
    actyxEvent: ActyxEvent<E>
  ): ActualitySnapshot<E> => ({
    t: Snapshot,
    self: () => actyxEvent,
    previous: () => previous(param, actyxEvent),
    root: () => root(param),
    canon: param.isCanon,
    expired: () => !param.isCanon(),
  });

  const actuality = <E>(param: ActualityParam<E>): Actuality<E> => {
    const self = () => param.canon.chain[0];
    return {
      t: Actual,
      self,
      previous: () => previous(param, self()),
      root: () => root(param),
      canon: param.isCanon,
      expired: () => !param.isCanon(),
    };
  };

  export const tracker = <E>(): Tracker<E> => {
    const internal: TrackerInternal<E> = {
      globalEventMap: new Map(),
      canon: null,
      expired: [],
      actualityCache: [],
    };

    const resolvePromises = (canon: Canon<E>) => {
      const cache = internal.actualityCache;
      if (Array.isArray(cache)) {
        cache.forEach((x) => x.resolve(canon));
        internal.actualityCache = createActuality(canon);
      }
    };

    const resetPromises = () => {
      const cache = internal.actualityCache;
      if (Array.isArray(cache)) {
        cache.forEach((x) => x.reject());
      }
      internal.actualityCache = [];
    };

    const createActuality = (canon: Canon<E>) =>
      actuality({
        canon,
        isCanon: () => internal.canon === canon,
      });

    const getActuality = () => {
      const cache = internal.actualityCache;
      if (Array.isArray(cache)) {
        return new Promise<Actuality<E>>((resolve, reject) => {
          cache.push({
            resolve: (canon) => resolve(createActuality(canon)),
            reject: () => reject(new Error("Failed getting Actuality")), //TODO: review when it may happen
          });
        });
      } else {
        return Promise.resolve(cache);
      }
    };

    const pushCanon = (e: ActyxEvent<E>): Canon<E> => {
      const canon: Canon<E> = internal.canon || {
        root: e,
        map: new Map(),
        chain: [],
      };
      internal.canon = canon;
      canon.map.set(e.meta.eventId, e);
      canon.chain.unshift(e);
      return canon;
    };

    const push: Tracker<E>["push"] = (e) => {
      internal.globalEventMap.set(e.meta.eventId, e);
      resolvePromises(pushCanon(e));
    };

    const expire = () => {
      const canon = internal.canon;
      if (canon) {
        internal.canon = null;
        internal.expired.push(canon);
        resetPromises();
      }
    };

    return {
      push,
      expire,
      getActuality,
    };
  };
}

export const run = async <E = unknown>(params: Params<E>) => {
  const actyx = await Actyx.of(...params.actyx);
  let cancel: CancelSubscription | null = null;

  const tracker = Actuality.tracker<E>();

  /**
   * Connect to Actyx with predefined params and keep connection until closed.
   */
  const connect = () => {
    if (cancel === null) {
      const onEventsOrTimeTravel: OnEventsOrTimetravel<E> = async (e) => {
        if (e.type === MsgType.timetravel) {
          tracker.expire();
        }

        if (e.type === MsgType.events) {
          e.events.map((e) => tracker.push(e));
        }
      };

      const onCompleteOrErr: OnCompleteOrErr = (err) => {
        if (err) {
          cancel = null;
          connect();
        }
      };

      cancel = actyx.subscribeMonotonic<E>(
        params.query,
        onEventsOrTimeTravel,
        onCompleteOrErr
      );
    }
  };

  connect();

  return {
    cancel: () => cancel?.(),
    getActuality: tracker.getActuality,
  };
};
