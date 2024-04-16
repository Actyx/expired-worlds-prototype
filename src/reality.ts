export const Snapshot: unique symbol = Symbol("Snapshot");
export type Snapshot = typeof Snapshot;

export const Actual: unique symbol = Symbol("Actual");
export type Actual = typeof Actual;

export const AmendmentsNotReady: unique symbol = Symbol("AmendmentsNotReady");
export type AmendmentsNotReady = typeof AmendmentsNotReady;

export type Amendment<T> = {
  past: Reality<T>;
  future: Reality<T>;
  divergentPoint: T;
};

/**
 * API that tracks a particular reality. Reality is defined as an array of `T`
 * which represents order of events.
 *
 * A Reality always involves a Witness as it is created when said Witness sees
 * `T` for the first time. Also, Reality expires when Witness "retrospect", in
 * which the next event seen by the Witness after the retrospect creates a new
 * Reality.
 */
export type Reality<T> = {
  t: Actual;
  /** Last `T` observed in this Reality */
  latest: () => Readonly<T>;
  /** All `T` observed in this Reality */
  history: () => ReadonlyArray<T>;
  /** Previous `T` as RealitySnapshot */
  previous: () => null | RealitySnapshot<T>;
  /** First `T` as RealitySnapshot */
  first: () => RealitySnapshot<T>;
  /**
   * @returns true if this reality is canonical
   */
  isCanon: () => boolean;
  /**
   * @returns true if this reality is expired
   */
  isExpired: () => boolean;
  /**
   * List of Amendment from this Reality to the canon reality.
   * If this Reality is canon, it will return an empty array.
   * If this Reality has not converged with the old reality, it will return `AmendmentsNotReady`
   */
  amendments: () => Amendment<T>[] | AmendmentsNotReady;
  /**
   * Get an amendment from current one to latest one if current is not canon.
   */
  directAmendments: () => Amendment<T> | AmendmentsNotReady | null;
};

/**
 * A view of Reality frozen in time. It will not change when the Witness sees
 * a new `T`.
 */
export type RealitySnapshot<T> = {
  t: Snapshot;
  latest: () => Readonly<T>;
  /** Previous `T` as RealitySnapshot */
  previous: () => null | RealitySnapshot<T>;
  /** First `T` as RealitySnapshot */
  first: () => RealitySnapshot<T>;
  /**
   * @returns true if this reality is canonical
   */
  isCanon: () => boolean;
  /**
   * @returns true if this reality is expired
   */
  isExpired: () => boolean;
  /**
   * @returns `Reality` of this snapshot
   */
  real: () => Reality<T>;
};

type Canon<T> = {
  map: Map<symbol, Data<T>>;
  chain: Data<T>[];
};

type Data<T> = [symbol, T];

type WitnessInternal<T> = {
  readonly globalDataTracker: Map<string, Data<T>>;
  canonReality: null | Canon<T>;
  expiredReality: Canon<T>[];
  realityMap: Map<Canon<T>, Reality<T>>;
  canonRealityCache:
    | { resolve: (item: Reality<T>) => void; reject: () => void }[]
    | Reality<T>;
};

type RealityParam<T> = {
  canon: Canon<T>;
  isCanon: () => boolean;
  amendments: () => Amendment<T>[] | AmendmentsNotReady;
  directAmendments: () => Amendment<T> | AmendmentsNotReady | null;
};

const root = <T>(param: RealityParam<T>, current: Reality<T>) =>
  snapshot(param, current, 0, param.canon.chain[0]);

const previous = <T>(
  param: RealityParam<T>,
  current: Reality<T>,
  currentIndex: number
) => {
  if (currentIndex === 0) return null;
  const previousIndex = currentIndex - 1;
  const item = param.canon.chain.at(previousIndex);
  if (!item) return null;
  return snapshot(param, current, previousIndex, item);
};

const snapshot = <T>(
  param: RealityParam<T>,
  reality: Reality<T>,
  currentIndex: number,
  currentItem: Data<T>
): RealitySnapshot<T> => {
  return {
    t: Snapshot,
    latest: () => currentItem[1],
    previous: () => previous(param, reality, currentIndex),
    first: () => root(param, reality),
    isCanon: param.isCanon,
    isExpired: () => !param.isCanon(),
    real: () => reality,
  };
};

const reality = <T>(param: RealityParam<T>): Reality<T> => {
  const self = () => param.canon.chain[param.canon.chain.length - 1];

  const reality: Reality<T> = {
    t: Actual,
    latest: () => self()[1],
    history: () => param.canon.chain.slice(0).map((x) => x[1]),
    previous: () => previous(param, reality, param.canon.chain.length - 1),
    first: () => root(param, reality),
    isCanon: param.isCanon,
    isExpired: () => !param.isCanon(),
    amendments: param.amendments,
    directAmendments: param.directAmendments,
  };

  return reality;
};

export type WitnessParams<T> = {
  id: (x: T) => string;
};

/**
 * Self explanatory. It is a witness that can see realities.
 */
export type Witness<T> = {
  /** See a `T` */
  see: (e: T) => void;
  /** Discard current reality. A new reality will be created the next time the Witness see a `T` */
  retrospect: () => void;
  /** Return the currently canon Reality */
  canonReality: () => Promise<Reality<T>>;
  /** Return expired past Realities */
  pastRealities: () => Reality<T>[];
};

export const witness = <T>(param: WitnessParams<T>): Witness<T> => {
  const internal: WitnessInternal<T> = {
    globalDataTracker: new Map(),
    canonReality: null,
    expiredReality: [],
    realityMap: new Map(),
    canonRealityCache: [],
  };

  const resolvePromises = (canon: Canon<T>) => {
    const cache = internal.canonRealityCache;
    if (Array.isArray(cache)) {
      const reality = initReality(canon);
      cache.forEach((x) => x.resolve(reality));
      internal.canonRealityCache = reality;
    }
  };

  const resetPromises = () => {
    const cache = internal.canonRealityCache;
    if (Array.isArray(cache)) {
      cache.forEach((x) => x.reject());
    }
    internal.canonRealityCache = [];
  };

  const initReality = (canon: Canon<T>) => {
    const isCanon = () => internal.canonReality === canon;
    const res = reality({
      canon,
      isCanon,
      amendments: () => {
        if (isCanon()) return [];
        const amendments: Amendment<T>[] = [];

        // TODO: canon is not caughtUp - AmendmentsNotReady

        for (
          let i = internal.expiredReality.indexOf(canon);
          i < internal.expiredReality.length;
          i++
        ) {
          const prevCanon = internal.expiredReality[i];
          const nextCanon =
            internal.expiredReality.at(i + 1) || internal.canonReality;
          if (!nextCanon) return AmendmentsNotReady;
          const prevReality = internal.realityMap.get(prevCanon);
          const nextReality = internal.realityMap.get(nextCanon);
          if (!prevReality || !nextReality) return AmendmentsNotReady;

          const divergentPoint = nextCanon.chain.find((x) =>
            prevCanon.map.has(x[0])
          );
          if (!divergentPoint) return AmendmentsNotReady;

          amendments.push({
            past: prevReality,
            future: nextReality,
            divergentPoint: divergentPoint[1],
          });
        }

        return amendments;
      },
      directAmendments: () => {
        let i = internal.expiredReality.indexOf(canon);
        if (i == -1) return null;
        const prevCanon = internal.expiredReality[i];
        const latestCanon = internal.canonReality;
        if (!latestCanon) return AmendmentsNotReady;
        const prevReality = internal.realityMap.get(prevCanon);
        const latestReality = internal.realityMap.get(latestCanon);
        if (!prevReality || !latestReality) return AmendmentsNotReady;

        const divergentPoint = latestCanon.chain.find((x) =>
          prevCanon.map.has(x[0])
        );
        if (!divergentPoint) return AmendmentsNotReady;

        return {
          past: prevReality,
          future: latestReality,
          divergentPoint: divergentPoint[1],
        };
      },
    });

    internal.realityMap.set(canon, res);

    return res;
  };

  const getCanonReality = () => {
    const cache = internal.canonRealityCache;
    if (Array.isArray(cache)) {
      return new Promise<Reality<T>>((resolve, reject) => {
        cache.push({
          resolve,
          reject: () => reject(new Error("Failed getting Reality")), //TODO: review when it may happen
        });
      });
    } else {
      return Promise.resolve(cache);
    }
  };

  const pushToCanon = (t: T): Canon<T> => {
    const id = param.id(t);
    const data: Data<T> = internal.globalDataTracker.get(param.id(t)) || [
      Symbol(),
      t,
    ];
    internal.globalDataTracker.set(id, data);

    const canon: Canon<T> = internal.canonReality || {
      map: new Map(),
      chain: [],
    };
    internal.canonReality = canon;

    canon.map.set(data[0], data);
    canon.chain.push(data);

    return canon;
  };

  const see: Witness<T>["see"] = (e) => {
    const canon = pushToCanon(e);
    resolvePromises(canon);
  };

  const expireCanon = () => {
    const canon = internal.canonReality;
    if (canon) {
      internal.canonReality = null;
      internal.expiredReality.push(canon);
      resetPromises();
    }
  };

  const witness: Witness<T> = {
    see,
    retrospect: expireCanon,
    canonReality: getCanonReality,
    pastRealities: () =>
      internal.expiredReality
        .slice(0)
        .map((x) => internal.realityMap.get(x))
        .filter((x): x is Reality<T> => x !== null),
  };

  return witness;
};
