import { describe, expect, it } from "@jest/globals";
import { Enum, sleep } from "./utils.js";
import {
  Code,
  Emit,
  Exact,
  One,
  Otherwise,
  Parallel,
  WFMachine,
} from "./wfmachine.js";
import { MakeCType } from "./consts.js";

// TODO:
// - Participations?

const Ev = Enum([
  "request",
  "bid",
  "cancelled",
  "assign",
  "accept",
  "notAccepted",
  "atSrc",
  "reqEnter",
  "doEnter",
  "deny",
  "inside",
  "withdrawn",
  "reqLeave",
  "doLeave",
  "success",
  "withdraw",
  "doLeave",
  "withdrawn",
  "loaded",
  "notPickedUp",
  "atDst",
  "unloaded",
  "reqStorage",
  "offerStorage",
  "assistanceNeeded",
  "atWarehouse",
  "stashed",
  "assistanceNeeded",
  "logisticFailed",
  "done",
] as const);
type Ev = Enum<typeof Ev>;

type TheType = MakeCType<{ role: "a"; ev: Ev }>;

describe("enums", () => {
  it("enums", () => {
    expect(Enum(["a", "b", "c"])).toEqual({
      a: "a",
      b: "b",
      c: "c",
    });
  });
});

describe("machine", () => {
  describe("event", () => {
    it("event", () => {
      const code = Code.make<TheType>();
      const machine = WFMachine<TheType>([
        code.event("a", Ev.request, {
          bindings: [code.bind("src", "from"), code.bind("dst", "to")],
        }),
      ]);

      expect(machine.returned()).toBe(false);

      machine.tick(
        Emit.event(Ev.request, { from: "storage-1", to: "storage-2" })
      );

      expect(machine.returned()).toBe(true);
      expect(machine.state()).toEqual({
        state: [One, Ev.request],
        context: { src: "storage-1", dst: "storage-2" },
      });
    });
  });

  describe("retry-timeout", () => {
    it("retry-timeout FAIL", async () => {
      const TIMEOUT_DURATION = 300;

      const code = Code.make<TheType>();

      const machine = WFMachine<TheType>([
        code.event("a", Ev.request, {
          bindings: [code.bind("src", "from"), code.bind("dst", "to")],
        }),
        ...code.retry([
          code.event("a", Ev.reqStorage, {
            bindings: [code.bind("somevar", "somefield")],
          }),
          ...code.timeout(
            TIMEOUT_DURATION,
            [code.event("a", Ev.bid)],
            code.event("a", Ev.cancelled, {
              control: Code.Control.fail,
            })
          ),
        ]),
      ]);

      expect(machine.returned()).toBe(false);
      machine.tick(
        Emit.event(Ev.request, { from: "storage-1", to: "storage-2" })
      );
      expect(machine.returned()).toBe(false);

      machine.tick(Emit.event(Ev.reqStorage, { somefield: "somevalue" }));
      expect(machine.returned()).toBe(false);
      expect(machine.state()).toEqual({
        state: [One, Ev.reqStorage],
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });
      expect(machine.availableTimeout()).toEqual([]);

      // attempt timeout will fail
      machine.tick(Emit.event(Ev.cancelled, {}));
      expect(machine.returned()).toBe(false);
      expect(machine.state()).toEqual({
        state: [One, Ev.reqStorage],
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });

      // after some moments some timeouts are available
      await sleep(TIMEOUT_DURATION + 100);
      expect(
        machine
          .availableTimeout()
          .findIndex(
            ({ consequence: { name, control } }) =>
              name === Ev.cancelled && control === Code.Control.fail
          ) !== -1
      ).toBe(true);

      // trigger timeout - state will be wound back to when RETRY
      machine.tick(Emit.event(Ev.cancelled, {}));
      expect(machine.returned()).toBe(false);
      expect(machine.state()).toEqual({
        state: [One, Ev.request],
        context: { src: "storage-1", dst: "storage-2" },
      });
    });

    it("retry-timeout RETURN", async () => {
      const TIMEOUT_DURATION = 300;
      const code = Code.make<TheType>();

      const machine = WFMachine<TheType>([
        code.event("a", Ev.request, {
          bindings: [code.bind("src", "from"), code.bind("dst", "to")],
        }),
        ...code.retry([
          code.event("a", Ev.reqStorage, {
            bindings: [code.bind("somevar", "somefield")],
          }),
          ...code.timeout(
            TIMEOUT_DURATION,
            [code.event("a", Ev.bid)],
            code.event("a", Ev.cancelled, { control: Code.Control.return })
          ),
        ]),
      ]);

      expect(machine.returned()).toBe(false);
      machine.tick(
        Emit.event(Ev.request, { from: "storage-1", to: "storage-2" })
      );
      expect(machine.returned()).toBe(false);

      machine.tick(Emit.event(Ev.reqStorage, { somefield: "somevalue" }));
      expect(machine.returned()).toBe(false);
      expect(machine.state()).toEqual({
        state: [One, Ev.reqStorage],
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });

      await sleep(TIMEOUT_DURATION + 100);
      // trigger timeout - also triggering return
      machine.tick(Emit.event(Ev.cancelled, {}));
      expect(machine.returned()).toBe(true);
      expect(machine.state()).toEqual({
        state: [One, Ev.cancelled],
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });
    });

    it("retry-timeout PASS", async () => {
      const TIMEOUT_DURATION = 300;
      const code = Code.make<TheType>();

      const machine = WFMachine<TheType>([
        code.event("a", Ev.request, {
          bindings: [code.bind("src", "from"), code.bind("dst", "to")],
        }),
        ...code.retry([
          code.event("a", Ev.reqStorage, {
            bindings: [code.bind("somevar", "somefield")],
          }),
          ...code.timeout(
            TIMEOUT_DURATION,
            [code.event("a", Ev.bid)],
            code.event("a", Ev.cancelled, { control: Code.Control.fail })
          ),
        ]),
      ]);

      expect(machine.returned()).toBe(false);
      machine.tick(
        Emit.event(Ev.request, { from: "storage-1", to: "storage-2" })
      );
      expect(machine.returned()).toBe(false);

      machine.tick(Emit.event(Ev.reqStorage, { somefield: "somevalue" }));
      expect(machine.returned()).toBe(false);
      expect(machine.state()).toEqual({
        state: [One, Ev.reqStorage],
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });

      machine.tick(Emit.event(Ev.bid, {}));

      await sleep(TIMEOUT_DURATION + 100);
      machine.tick(null);
      expect(machine.state()).toEqual({
        state: [One, Ev.bid],
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });
    });
  });

  describe("parallel", () => {
    it("produce parallel state and work the next workable code and event", () => {
      const code = Code.make<TheType>();
      const machine = WFMachine<TheType>([
        code.event("a", Ev.request),
        ...code.parallel({ min: 2 }, [code.event("a", Ev.bid)]), // minimum of two bids
        ...code.retry([code.event("a", Ev.accept)]), // test that next code is not immediately event too
      ]);

      machine.tick(Emit.event(Ev.request, {}));
      expect(machine.state()).toEqual({
        state: [One, Ev.request],
        context: {},
      });

      machine.tick(Emit.event(Ev.bid, {}));
      expect(machine.state()).toEqual({
        state: [Parallel, [Ev.bid]],
        context: {},
      });

      machine.tick(Emit.event(Ev.accept, {})); // attempt to accept will fail because parallel count isn't fulfilled
      expect(machine.state()).toEqual({
        state: [Parallel, [Ev.bid]],
        context: {},
      });

      machine.tick(Emit.event(Ev.bid, {})); // the second bid
      expect(machine.state()).toEqual({
        state: [Parallel, [Ev.bid, Ev.bid]],
        context: {},
      });

      machine.tick(Emit.event(Ev.accept, {})); // finally accept should work
      expect(machine.state()).toEqual({
        state: [One, Ev.accept],
        context: {},
      });
    });

    it("works with choice", () => {
      const code = Code.make<TheType>();
      const machine = WFMachine<TheType>([
        code.event("a", Ev.request),
        ...code.parallel({ min: 0 }, [code.event("a", Ev.bid)]), // minimum of two bids
        ...code.choice([code.event("a", Ev.accept), code.event("a", Ev.deny)]),
      ]);

      machine.tick(Emit.event(Ev.request, {}));
      machine.tick(Emit.event(Ev.deny, {}));
      expect(machine.state()).toEqual({
        state: [One, Ev.deny],
        context: {},
      });
    });

    it("sequence of parallels", () => {
      const code = Code.make<TheType>();
      const machine = WFMachine<TheType>([
        code.event("a", Ev.request),
        ...code.parallel({ min: 2 }, [code.event("a", Ev.bid)]), // minimum of two bids
        ...code.parallel({ min: 2 }, [code.event("a", Ev.accept)]), // minimum of two bids
      ]);

      machine.tick(Emit.event(Ev.request, {}));
      machine.tick(Emit.event(Ev.bid, {}));
      expect(machine.state().state).toEqual([Parallel, [Ev.bid]]);
      machine.tick(Emit.event(Ev.bid, {}));
      expect(machine.state().state).toEqual([Parallel, [Ev.bid, Ev.bid]]);
      machine.tick(Emit.event(Ev.accept, {}));
      expect(machine.state().state).toEqual([Parallel, [Ev.accept]]);
      machine.tick(Emit.event(Ev.accept, {}));
      expect(machine.state().state).toEqual([Parallel, [Ev.accept, Ev.accept]]);
    });

    it("max reached force next", () => {
      const code = Code.make<TheType>();
      const machine = WFMachine<TheType>([
        code.event("a", Ev.request),
        ...code.parallel({ min: 1, max: 2 }, [code.event("a", Ev.bid)]), // minimum of two bids
      ]);

      machine.tick(Emit.event(Ev.request, {}));
      machine.tick(Emit.event(Ev.bid, {}));
      expect(machine.state().state).toEqual([Parallel, [Ev.bid]]);
      machine.tick(Emit.event(Ev.bid, {}));
      expect(machine.state().state).toEqual([Parallel, [Ev.bid, Ev.bid]]);
      expect(machine.returned()).toBe(true);
    });
  });

  describe("compensation", () => {
    it("passing without compensation", () => {
      const code = Code.make<TheType>();
      const machine = WFMachine<TheType>([
        code.event("a", Ev.inside, {}),
        ...code.compensate(
          [
            code.event("a", Ev.reqLeave),
            code.event("a", Ev.doLeave),
            code.event("a", Ev.success),
          ],
          [
            code.event("a", Ev.withdraw),
            code.event("a", Ev.doLeave),
            code.event("a", Ev.withdrawn),
          ]
        ),
      ]);

      machine.tick(Emit.event(Ev.inside, {}));
      expect(machine.state().state).toEqual([One, Ev.inside]);

      machine.tick(Emit.event(Ev.reqLeave, {}));
      expect(machine.state().state).toEqual([One, Ev.reqLeave]);
      expect(machine.availableCompensateable()).toEqual([
        { name: Ev.withdraw, role: "a" },
      ]);

      machine.tick(Emit.event(Ev.doLeave, {}));
      expect(machine.state().state).toEqual([One, Ev.doLeave]);
      expect(machine.availableCompensateable()).toEqual([
        { name: Ev.withdraw, role: "a" },
      ]);

      machine.tick(Emit.event(Ev.success, {}));
      expect(machine.state().state).toEqual([One, Ev.success]);
      expect(machine.availableCompensateable()).toEqual([]);
      expect(machine.returned()).toEqual(true);
    });

    it("passing with compensation", () => {
      const code = Code.make<TheType>();
      const machine = WFMachine<TheType>([
        code.event("a", Ev.inside, {}),
        ...code.compensate(
          [
            code.event("a", Ev.reqLeave),
            code.event("a", Ev.doLeave),
            code.event("a", Ev.success),
          ],
          [
            code.event("a", Ev.withdraw),
            code.event("a", Ev.doLeave),
            code.event("a", Ev.withdrawn),
          ]
        ),
      ]);

      machine.tick(Emit.event(Ev.inside, {}));
      expect(machine.state().state).toEqual([One, Ev.inside]);

      machine.tick(Emit.event(Ev.reqLeave, {}));
      expect(machine.state().state).toEqual([One, Ev.reqLeave]);
      expect(machine.availableCompensateable()).toEqual([
        { name: Ev.withdraw, role: "a" },
      ]);

      machine.tick(Emit.event(Ev.withdraw, {}));
      expect(machine.state().state).toEqual([One, Ev.withdraw]);
      expect(machine.availableCompensateable()).toEqual([]);
      machine.tick(Emit.event(Ev.doLeave, {}));
      expect(machine.state().state).toEqual([One, Ev.doLeave]);
      machine.tick(Emit.event(Ev.withdrawn, {}));
      expect(machine.state().state).toEqual([One, Ev.withdrawn]);
      expect(machine.returned()).toEqual(true);
    });
  });

  describe("match", () => {
    it("named match should work", () => {
      const code = Code.make<TheType>();
      const machine = WFMachine<TheType>([
        code.event("a", Ev.request),
        ...code.match(
          [
            code.event("a", Ev.inside),
            ...code.compensate(
              [
                code.event("a", Ev.reqLeave),
                code.event("a", Ev.doLeave),
                code.event("a", Ev.success),
              ],
              [
                code.event("a", Ev.withdraw),
                code.event("a", Ev.doLeave),
                code.event("a", Ev.withdrawn),
              ]
            ),
          ],
          [
            code.matchCase([Exact, Ev.success], []),
            code.matchCase(
              [Otherwise],
              [code.event("a", Ev.cancelled, { control: Code.Control.return })]
            ),
          ]
        ),
        code.event("a", Ev.done),
      ]);

      machine.tick(Emit.event(Ev.request, {}));
      machine.tick(Emit.event(Ev.inside, {}));
      machine.tick(Emit.event(Ev.reqLeave, {}));
      machine.tick(Emit.event(Ev.doLeave, {}));
      machine.tick(Emit.event(Ev.success, {}));
      machine.tick(Emit.event(Ev.done, {}));
      expect(machine.returned()).toBe(true);
      expect(machine.state().state).toEqual([One, Ev.done]);
    });

    it("otherwise match should work", () => {
      const code = Code.make<TheType>();
      const machine = WFMachine<TheType>([
        code.event("a", Ev.request),
        ...code.match(
          [
            code.event("a", Ev.inside),
            ...code.compensate(
              [
                code.event("a", Ev.reqLeave),
                code.event("a", Ev.doLeave),
                code.event("a", Ev.success),
              ],
              [
                code.event("a", Ev.withdraw),
                code.event("a", Ev.doLeave),
                code.event("a", Ev.withdrawn),
              ]
            ),
          ],
          [
            code.matchCase([Exact, Ev.success], []),
            code.matchCase(
              [Otherwise],
              [code.event("a", Ev.cancelled, { control: Code.Control.return })]
            ),
          ]
        ),
        code.event("a", Ev.done),
      ]);

      machine.tick(Emit.event(Ev.request, {}));
      machine.tick(Emit.event(Ev.inside, {}));
      machine.tick(Emit.event(Ev.reqLeave, {}));
      machine.tick(Emit.event(Ev.doLeave, {}));
      machine.tick(Emit.event(Ev.withdraw, {}));
      machine.tick(Emit.event(Ev.doLeave, {}));
      machine.tick(Emit.event(Ev.withdrawn, {}));
      machine.tick(Emit.event(Ev.cancelled, {}));
      expect(machine.returned()).toBe(true);
      expect(machine.state().state).toEqual([One, Ev.cancelled]);
    });
  });

  describe("choice", () => {
    const code = Code.make<TheType>();
    const prepare = () =>
      WFMachine<TheType>([
        code.event("a", Ev.request),
        ...code.choice([
          code.event("a", Ev.accept),
          code.event("a", Ev.deny, { control: Code.Control.return }),
          code.event("a", Ev.assistanceNeeded, {
            control: Code.Control.return,
          }),
        ]),
        code.event("a", Ev.doEnter),
      ]);

    it("should work for any event inside the choice - 1", () => {
      const machine = prepare();
      machine.tick(Emit.event(Ev.request, {}));
      machine.tick(Emit.event(Ev.accept, {}));
      expect(machine.returned()).toEqual(false);
      expect(machine.state().state).toEqual([One, Ev.accept]);
      machine.tick(Emit.event(Ev.doEnter, {}));
      expect(machine.returned()).toEqual(true);
      expect(machine.state().state).toEqual([One, Ev.doEnter]);
    });

    it("should work for any event inside the choice - 2", () => {
      const machine = prepare();
      machine.tick(Emit.event(Ev.request, {}));
      machine.tick(Emit.event(Ev.deny, {}));
      expect(machine.returned()).toEqual(true);
      expect(machine.state().state).toEqual([One, Ev.deny]);
    });

    it("should work for any event inside the choice - 3", () => {
      const machine = prepare();
      machine.tick(Emit.event(Ev.request, {}));
      machine.tick(Emit.event(Ev.assistanceNeeded, {}));
      expect(machine.returned()).toEqual(true);
      expect(machine.state().state).toEqual([One, Ev.assistanceNeeded]);
    });
  });

  describe("commands", () => {
    type MultiRole = MakeCType<{
      role: "transporter" | "storage" | "manager";
      ev: Ev;
    }>;

    it("should work with event", () => {
      const code = Code.make<MultiRole>();
      const machine = WFMachine([code.event("transporter", "bid")]);
      expect(machine.availableCommands()).toContainEqual({
        role: "transporter",
        name: "bid",
        reason: null,
      });
    });

    it("should work with choice", () => {
      const code = Code.make<MultiRole>();
      const machine = WFMachine([
        code.event("transporter", "bid"),
        ...code.choice([
          code.event("storage", "accept"),
          code.event("manager", "deny"),
        ]),
      ]);

      machine.tick(Emit.event(Ev.bid, {}));

      expect(machine.availableCommands()).toContainEqual({
        role: "storage",
        name: "accept",
        reason: null,
      });

      expect(machine.availableCommands()).toContainEqual({
        role: "manager",
        name: "deny",
        reason: null,
      });
    });

    it("should work with parallel", () => {
      const code = Code.make<MultiRole>();
      const machine = WFMachine([
        code.event("manager", Ev.request),
        ...code.parallel({ min: 1, max: 2 }, [
          code.event("transporter", Ev.bid),
        ]),
        code.event("manager", Ev.accept),
      ]);

      machine.tick(Emit.event(Ev.request, {}));

      // min not reached
      expect(machine.availableCommands()).toContainEqual({
        role: "transporter",
        name: Ev.bid,
        reason: null,
      });

      expect(machine.availableCommands()).not.toContainEqual({
        role: "manager",
        name: Ev.accept,
        reason: null,
      });

      // min reached - maxx not reached
      machine.tick(Emit.event(Ev.bid, {}));
      expect(machine.availableCommands()).toContainEqual({
        role: "transporter",
        name: Ev.bid,
        reason: null,
      });

      expect(machine.availableCommands()).toContainEqual({
        role: "manager",
        name: Ev.accept,
        reason: null,
      });

      // max reached
      machine.tick(Emit.event(Ev.bid, {}));
      expect(machine.availableCommands()).not.toContainEqual({
        role: "transporter",
        name: Ev.bid,
        reason: null,
      });

      expect(machine.availableCommands()).toContainEqual({
        role: "manager",
        name: Ev.accept,
        reason: null,
      });
    });

    it("should work with timeout", async () => {
      const TIMEOUT_DURATION = 100;
      const code = Code.make<MultiRole>();
      const machine = WFMachine([
        code.event("manager", Ev.request, {
          bindings: [],
        }),
        ...code.timeout(
          TIMEOUT_DURATION,
          [...code.parallel({ max: 2 }, [code.event("transporter", Ev.bid)])],
          code.event("manager", Ev.cancelled, { control: Code.Control.fail })
        ),
      ]);

      expect(machine.availableCommands()).toContainEqual({
        control: undefined,
        role: "manager",
        name: Ev.request,
        reason: null,
      });
      machine.tick(Emit.event(Ev.request, {}));

      await sleep(TIMEOUT_DURATION + 1);

      expect(machine.availableCommands()).toContainEqual({
        control: undefined,
        role: "transporter",
        name: Ev.bid,
        reason: null,
      });
      expect(machine.availableCommands()).toContainEqual({
        role: "manager",
        name: Ev.cancelled,
        control: "fail",
        reason: "timeout",
      });
    });

    // // TODO: alan: I'm not clear on how compensation should actually work
    // it("should work with compensation", async () => {
    // });

    it("should work match subworkflow", () => {
      const code = Code.make<MultiRole>();
      const machine = WFMachine([
        code.event("transporter", Ev.accept),
        ...code.match(
          [
            code.event("transporter", Ev.reqEnter),
            ...code.choice([
              code.event("storage", Ev.doEnter),
              code.event("transporter", Ev.withdraw, { control: "return" }),
            ]),
            code.event("transporter", Ev.inside),
          ],
          [
            code.matchCase(
              [Exact, Ev.inside],
              [code.event("manager", Ev.success)]
            ),
            code.matchCase([Otherwise], [code.event("manager", Ev.cancelled)]),
          ]
        ),
      ]);

      machine.tick(Emit.event(Ev.accept, {}));
      machine.tick(Emit.event(Ev.reqEnter, {}));

      expect(machine.availableCommands()).toContainEqual({
        role: "storage",
        name: Ev.doEnter,
        control: undefined,
        reason: null,
      });

      expect(machine.availableCommands()).toContainEqual({
        role: "transporter",
        name: Ev.withdraw,
        control: "return",
        reason: null,
      });

      machine.tick(Emit.event(Ev.doEnter, {}));
      machine.tick(Emit.event(Ev.inside, {}));

      expect(machine.availableCommands()).toContainEqual({
        role: "manager",
        name: Ev.success,
        control: undefined,
        reason: null,
      });
    });
  });
});
