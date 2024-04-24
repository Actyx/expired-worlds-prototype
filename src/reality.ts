export const Snapshot: unique symbol = Symbol("Snapshot");
export type Snapshot = typeof Snapshot;

export const Actual: unique symbol = Symbol("Actual");
export type Actual = typeof Actual;

export const AmendmentsNotReady: unique symbol = Symbol("AmendmentsNotReady");
export type AmendmentsNotReady = typeof AmendmentsNotReady;

export type Amendment<T> = {
  past: Reality<T>;
  future: Reality<T>;
  divergentPoint: T | null;
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
  latest: () => null | Readonly<T>;
  /** All `T` observed in this Reality */
  history: () => ReadonlyHistory<T>;
  /** Previous `T` as RealitySnapshot */
  previous: () => null | RealitySnapshot<T>;
  /** First `T` as RealitySnapshot */
  first: () => null | RealitySnapshot<T>;
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
  latest: () => null | Readonly<T>;
  /** Previous `T` as RealitySnapshot */
  previous: () => null | RealitySnapshot<T>;
  /** First `T` as RealitySnapshot */
  first: () => null | RealitySnapshot<T>;
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

type History<T> = {
  map: Map<symbol, Data<T>>;
  chain: Data<T>[];
};

type ReadonlyHistory<T> = {
  map: ReadonlyMap<symbol, Data<T>>;
  chain: ReadonlyArray<Data<T>>;
};

type Data<T> = [symbol, T];

type WitnessInternal<T> = {
  readonly multiversalDataTracker: Map<string, Data<T>>;
  getCanonReality: () => Reality<T>;
  expiredRealities: Reality<T>[];
};

type RealityInternals<T> = {
  history: History<T>;
  isCanon: () => boolean;
  amendments: () => Amendment<T>[] | AmendmentsNotReady;
  directAmendments: () => Amendment<T> | AmendmentsNotReady | null;
};

const root = <T>(param: RealityInternals<T>, current: Reality<T>) =>
  snapshot(param, current, 0, param.history.chain[0]);

const previous = <T>(
  param: RealityInternals<T>,
  current: Reality<T>,
  currentIndex: number
) => {
  if (currentIndex === 0) return null;
  const previousIndex = currentIndex - 1;
  const item = param.history.chain.at(previousIndex);
  if (!item) return null;
  return snapshot(param, current, previousIndex, item);
};

const snapshot = <T>(
  internals: RealityInternals<T>,
  reality: Reality<T>,
  currentIndex: number,
  currentItem: Data<T>
): RealitySnapshot<T> => {
  return {
    t: Snapshot,
    latest: () => currentItem[1],
    previous: () => previous(internals, reality, currentIndex),
    first: () => root(internals, reality),
    isCanon: internals.isCanon,
    isExpired: () => !internals.isCanon(),
    real: () => reality,
  };
};

const reality = <T>(internals: RealityInternals<T>): Reality<T> => {
  const self = () =>
    internals.history.chain[internals.history.chain.length - 1];

  const reality: Reality<T> = {
    t: Actual,
    latest: () => self()[1],
    history: () => ({
      chain: [...internals.history.chain],
      map: new Map(internals.history.map),
    }),
    previous: () =>
      previous(internals, reality, internals.history.chain.length - 1),
    first: () => root(internals, reality),
    isCanon: internals.isCanon,
    isExpired: () => !internals.isCanon(),
    amendments: internals.amendments,
    directAmendments: internals.directAmendments,
  };

  return reality;
};

const createRealityControl = <T>(witnessInternal: WitnessInternal<T>) => {
  const history: History<T> = { map: new Map(), chain: [] };
  const isCanon = (): boolean =>
    thisReality === witnessInternal.getCanonReality();
  const thisReality = reality<T>({
    history,
    isCanon,
    amendments: () => {
      if (isCanon()) return [];
      const amendments: Amendment<T>[] = [];

      // TODO: canon is not caughtUp - AmendmentsNotReady

      const indexOfThisReality =
        witnessInternal.expiredRealities.indexOf(thisReality);

      for (
        let i = indexOfThisReality;
        i < witnessInternal.expiredRealities.length;
        i++
      ) {
        const prevReality = witnessInternal.expiredRealities[i];
        const nextReality =
          witnessInternal.expiredRealities.at(i + 1) ||
          witnessInternal.getCanonReality();

        // TODO: review logic, currently too confused to do it
        if (!prevReality || !nextReality) return AmendmentsNotReady;
        const divergentPoint =
          history.chain.find((ev) => history.map.has(ev[0])) || null;

        const amendment: Amendment<T> = {
          past: prevReality,
          future: nextReality,
          divergentPoint: divergentPoint?.[1] || null,
        };

        amendments.push(amendment);
      }

      return amendments;
    },
    directAmendments: () => {
      let i = witnessInternal.expiredRealities.indexOf(thisReality);
      if (i == -1) return null;
      const prevReality = witnessInternal.expiredRealities[i];
      const canonReality = witnessInternal.getCanonReality();

      // TODO: review logic
      if (!canonReality) return AmendmentsNotReady;
      if (!prevReality) return AmendmentsNotReady;

      const divergentPoint =
        history.chain.find((ev) => history.map.has(ev[0])) || null;

      const amendment: Amendment<T> = {
        past: prevReality,
        future: canonReality,
        divergentPoint: divergentPoint?.[1] || null,
      };

      return amendment;
    },
  });

  return {
    push: (data: Data<T>) => {
      history.map.set(data[0], data);
      history.chain.push(data);
      // const id = param.id(t);
      // const data: Data<T> = internal.globalDataTracker.get(param.id(t)) || [
      //   Symbol(),
      //   t,
      // ];
      // internal.globalDataTracker.set(id, data);
      // const canon: History<T> = internal.getCanonReality || {
      //   map: new Map(),
      //   chain: [],
      // };
      // internal.getCanonReality = canon;
      // canon.map.set(data[0], data);
      // canon.chain.push(data);
      // return canon;
    },
    reality: thisReality,
  };
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
  retrospect: () => Reality<T> | null;
  /** Return the currently canon Reality */
  canonReality: () => Reality<T>;
  /** Return expired past Realities */
  pastRealities: () => Reality<T>[];
};

export const witness = <T>(param: WitnessParams<T>): Witness<T> => {
  const internal: WitnessInternal<T> = {
    multiversalDataTracker: new Map(),
    getCanonReality: () => {
      if (!canonRealityControl) throw new Error("should be impossible");
      return canonRealityControl.reality;
    },
    expiredRealities: [],
  };

  let canonRealityControl = createRealityControl<T>(internal);

  // internal functions

  const registerToMultiverse = (t: T): Data<T> => {
    const id = param.id(t);
    const data: Data<T> = internal.multiversalDataTracker.get(param.id(t)) || [
      Symbol(),
      t,
    ];
    internal.multiversalDataTracker.set(id, data);
    return data;
  };

  // methods

  const see: Witness<T>["see"] = (t: T) => {
    const data = registerToMultiverse(t);
    canonRealityControl.push(data);
  };

  const expireCanon = () => {
    const oldRealityControl = canonRealityControl;
    canonRealityControl = createRealityControl(internal);
    internal.expiredRealities.push(oldRealityControl.reality);
    return null;
  };

  const witness: Witness<T> = {
    see,
    retrospect: expireCanon,
    canonReality: internal.getCanonReality,
    pastRealities: () => [...internal.expiredRealities],
  };

  return witness;
};
