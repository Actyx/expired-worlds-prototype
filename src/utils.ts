export const sleep = (x: number) => new Promise((res) => setTimeout(res, x));

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
  type Inner<P extends Proto> = [P["t"], P["val"]];

  export type Type<P extends Proto> = Readonly<{
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
      const build: BlueprintIntermediate<P>["build"] = () => (val) => {
        const inner: Inner<P> = [t, val];
        return {
          t,
          set: (val) => (inner[1] = val),
          get: () => inner[1],
        };
      };

      return { build, refine };
    };

    return blueprintInner<{ t: T; val: any }>();
  };
}
