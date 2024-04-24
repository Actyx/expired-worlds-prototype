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
