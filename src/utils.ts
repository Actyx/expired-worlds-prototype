export const sleep = (x: number) => new Promise((res) => setTimeout(res, x));

export const cleanup = () => {
  let set = new Set<Function>();
  const self = {
    add: (fn: Function) => {
      const theSet = set;
      theSet.add(fn);
      return () => theSet.delete(fn);
    },
    clean: () => {
      const oldset = set;
      set = new Set();
      Array.from(oldset).forEach((fn) => fn());
    },
  };
  return self;
};

export const Enum = <T extends ReadonlyArray<string>>(
  strs: T
): Readonly<{ [S in T[number] as S]: Readonly<S> }> =>
  strs.reduce((acc, x) => {
    acc[x] = x;
    return acc;
  }, {} as any);
export type Enum<T extends object> = T[keyof T];

export type ExcludeArrayMember<T extends any[], Excludable> = Exclude<
  T[any],
  Excludable
>[];

export namespace NumArrToCodepoint {
  export const nts = (a: number[]) =>
    a.map((x) => String.fromCodePoint(x)).join("");
}

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

  export const toNum = (ord: Ord.Type): number => {
    switch (ord) {
      case Ord.Greater:
        return 1;
      case Ord.Lesser:
        return -1;
      default:
        return 0;
    }
  };

  export const ofString = (a: string, b: string): Type => {
    switch (true) {
      case a > b:
        return Ord.Greater;
      case a < b:
        return Ord.Lesser;
      default:
        return Ord.Equal;
    }
  };

  export const fromNum = (number: number): Type => {
    switch (true) {
      case number > 0:
        return Ord.Greater;
      case number < 0:
        return Ord.Lesser;
      default:
        return Ord.Equal;
    }
  };
}

export namespace WrapType {
  type Proto = {
    t: string;
    val: any;
  };
  export type Inner<P extends Proto> = {
    t: P["t"];
    value: ["val"];
  };

  export type Type<P extends Proto> = Readonly<{
    data: Inner<P>;
    t: P["t"];
    set: (t: P["val"]) => void;
    get: () => P["val"];
  }>;

  type BlueprintIntermediate<P extends Proto> = {
    refine: <Val extends P["val"]>() => BlueprintIntermediate<{
      t: P["t"];
      val: Val;
    }>;
    build: () => Blueprint<P>;
  };

  export namespace Utils {
    export type RefineProto<P extends Proto, Val extends P["val"]> = {
      t: P["t"];
      val: Val;
    };

    export type Refine<
      B extends BlueprintIntermediate<any>,
      Val extends ProtoOf<B>["val"]
    > = B extends BlueprintIntermediate<infer P>
      ? BlueprintIntermediate<RefineProto<P, Val>>
      : never;

    export type ProtoOf<B extends Blueprint<any> | BlueprintIntermediate<any>> =
      B extends Blueprint<infer P>
        ? P
        : B extends BlueprintIntermediate<infer P>
        ? P
        : Proto;
  }

  export type Blueprint<P extends Proto> = (val: Proto["val"]) => Type<P>;

  export type TypeOf<B extends Blueprint<any> | BlueprintIntermediate<any>> =
    Type<Utils.ProtoOf<B>>;

  export const blueprint = <T extends Proto["t"]>(t: T) => {
    const blueprintInner = <P extends Proto>(): BlueprintIntermediate<P> => {
      const refine: BlueprintIntermediate<P>["refine"] = blueprintInner;
      const build: BlueprintIntermediate<P>["build"] = () => (value) => {
        const self: Type<P> = {
          data: { t, value: value },
          t,
          set: (val) => (self.data.value = val),
          get: () => self.data.value,
        };
        return self;
      };

      return { build, refine };
    };

    return blueprintInner<{ t: T; val: any }>();
  };
}

export type Logger = ReturnType<typeof makeLogger>;
export const makeLogger = (
  id: string = String(Math.round(Math.random() * 10000))
) => {
  type Args = any[];
  type Sub = (...args: Args) => unknown;
  const subs = new Set<Sub>();
  return {
    sub: (fn: Sub) => {
      subs.add(fn);
      return () => subs.delete(fn);
    },
    unsub: (fn: Sub) => subs.delete(fn),
    log: (...args: Args) => {
      subs.forEach((sub) => sub(id, ...args));
    },
  };
};

export namespace MultihashMap {
  type ValidKeyMemberType =
    | string
    | boolean
    | number
    | undefined
    | null
    | symbol;
  export type KeyParam = ValidKeyMemberType[] | readonly ValidKeyMemberType[];
  export type KeyOf<T extends Type<any, any, any>> = T extends Type<
    infer K,
    any,
    any
  >
    ? K
    : never;

  export type ValueOf<T extends Type<any, any, any>> = T extends Type<
    any,
    infer V,
    any
  >
    ? V
    : never;

  export type Type<
    Key extends KeyParam & { length: Depth },
    Value,
    Depth extends number = Key["length"]
  > = {
    [Symbol.iterator]: Map<Key, Value>[typeof Symbol.iterator];
    get: (k: Key) => Value | undefined;
    has: (k: Key) => boolean;
    set: (k: Key, value: Value) => void;
    values: Map<Key, Value>["values"];
    keys: Map<Key, Value>["keys"];
    entries: Map<Key, Value>["entries"];
    clear: Map<Key, Value>["clear"];
    forEach: Map<Key, Value>["forEach"];
  };

  export const depth = <Depth extends number>(depth: Depth) => ({
    make: <Key extends KeyParam & { length: Depth }, Value>(): Type<
      Key,
      Value,
      Depth
    > => {
      if (depth < 1) {
        throw new Error("cannot create multihash map with depth < 1");
      }

      const storedKeys: Key[] = [];
      const storeKey = (key: Key) => {
        const newKey = [...key] as Key;
        storedKeys.push(newKey);
        return newKey;
      };
      const findOrMakeKey = (inputKey: Key, remember = false): Key => {
        if (inputKey.length !== depth) return inputKey;
        const key = storedKeys.find(
          (key) => key.findIndex((member, i) => inputKey[i] !== member) === -1
        );

        if (key !== undefined) return key;
        if (!remember) return inputKey;
        return storeKey(inputKey);
      };

      const map = new Map<Key, Value>();

      const self: Type<Key, Value, Depth> = {
        /** Returns an iterable of entries in the map. */
        [Symbol.iterator]: () => map[Symbol.iterator](),
        get: (inputKey) => map.get(findOrMakeKey(inputKey)),
        set: (inputKey, value) => {
          map.set(findOrMakeKey(inputKey, true), value);
        },
        has: (inputKey) => map.has(findOrMakeKey(inputKey)),
        entries: () => map.entries(),
        values: () => map.values(),
        keys: () => map.keys(),
        clear: () => {
          storedKeys.length = 0;
          map.clear();
        },
        forEach: (...args) => map.forEach(...args),
      };

      return self;
    },
  });
}

export namespace LazyMap {
  export type Type<Key, Val> = ReturnType<typeof make<Key, Val>>;
  export const make = <Key, Val>(gen: (key: Key) => Val) => {
    const map = new Map<Key, Val>();

    return {
      get: (key: Key) => {
        const val = map.get(key) || gen(key);
        map.set(key, val);
        return val;
      },
    };
  };
}
