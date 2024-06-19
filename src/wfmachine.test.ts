import { describe, expect, it } from "@jest/globals";
import { Enum, sleep } from "./utils.js";
import { Emit, One, Parallel, WFMachine } from "./wfmachine.js";
import { MakeCType } from "./consts.js";
import { Code } from "./wfcode.js";

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
const id = "id";

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
      const { role } = code.actor;
      const machine = WFMachine<TheType>(
        { id },
        {
          code: [
            code.event(role("a"), Ev.request, {
              bindings: [code.bind("src", "from"), code.bind("dst", "to")],
            }),
          ] as const,
          uniqueParams: [],
        }
      );

      expect(machine.returned()).toBe(false);

      machine.tick(
        Emit.event(id, Ev.request, { from: "storage-1", to: "storage-2" })
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
      const { role } = code.actor;

      const machine = WFMachine<TheType>(
        { id },
        {
          uniqueParams: [],
          code: [
            code.event(role("a"), Ev.request, {
              bindings: [code.bind("src", "from"), code.bind("dst", "to")],
            }),
            ...code.retry([
              code.event(role("a"), Ev.reqStorage, {
                bindings: [code.bind("somevar", "somefield")],
              }),
              ...code.timeout(
                TIMEOUT_DURATION,
                [code.event(role("a"), Ev.bid)],
                code.event(role("a"), Ev.cancelled, {
                  control: Code.Control.fail,
                })
              ),
            ]),
          ],
        }
      );

      expect(machine.returned()).toBe(false);
      machine.tick(
        Emit.event(id, Ev.request, { from: "storage-1", to: "storage-2" })
      );
      expect(machine.returned()).toBe(false);

      machine.tick(Emit.event(id, Ev.reqStorage, { somefield: "somevalue" }));
      expect(machine.returned()).toBe(false);
      expect(machine.state()).toEqual({
        state: [One, Ev.reqStorage],
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });

      expect(
        machine.availableTimeouts().find((x) => {
          const actor = x.consequence.actor;
          return (
            actor.t === "Role" &&
            actor.get() === "a" &&
            x.consequence.name === "cancelled"
          );
        })
      ).toBeTruthy();

      // trigger timeout - state will be wound back to when RETRY
      machine.tick(Emit.event(id, Ev.cancelled, {}));
      expect(machine.returned()).toBe(false);
      expect(machine.state()).toEqual({
        state: [One, Ev.request],
        context: { src: "storage-1", dst: "storage-2" },
      });
    });

    it("retry-timeout RETURN", async () => {
      const TIMEOUT_DURATION = 300;
      const code = Code.make<TheType>();
      const { role } = code.actor;

      const machine = WFMachine<TheType>(
        { id },
        {
          uniqueParams: [],
          code: [
            code.event(role("a"), Ev.request, {
              bindings: [code.bind("src", "from"), code.bind("dst", "to")],
            }),
            ...code.retry([
              code.event(role("a"), Ev.reqStorage, {
                bindings: [code.bind("somevar", "somefield")],
              }),
              ...code.timeout(
                TIMEOUT_DURATION,
                [code.event(role("a"), Ev.bid)],
                code.event(role("a"), Ev.cancelled, {
                  control: Code.Control.return,
                })
              ),
            ]),
          ],
        }
      );

      expect(machine.returned()).toBe(false);
      machine.tick(
        Emit.event(id, Ev.request, { from: "storage-1", to: "storage-2" })
      );
      expect(machine.returned()).toBe(false);

      machine.tick(Emit.event(id, Ev.reqStorage, { somefield: "somevalue" }));
      expect(machine.returned()).toBe(false);
      expect(machine.state()).toEqual({
        state: [One, Ev.reqStorage],
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });

      await sleep(TIMEOUT_DURATION + 100);
      // trigger timeout - also triggering return
      machine.tick(Emit.event(id, Ev.cancelled, {}));
      expect(machine.returned()).toBe(true);
      expect(machine.state()).toEqual({
        state: [One, Ev.cancelled],
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });
    });

    it("retry-timeout PASS", async () => {
      const TIMEOUT_DURATION = 300;
      const code = Code.make<TheType>();
      const { role } = code.actor;

      const machine = WFMachine<TheType>({ id }, {
        uniqueParams: [],
        code: [
          code.event(role("a"), Ev.request, {
            bindings: [code.bind("src", "from"), code.bind("dst", "to")],
          }),
          ...code.retry([
            code.event(role("a"), Ev.reqStorage, {
              bindings: [code.bind("somevar", "somefield")],
            }),
            ...code.timeout(
              TIMEOUT_DURATION,
              [code.event(role("a"), Ev.bid)],
              code.event(role("a"), Ev.cancelled, {
                control: Code.Control.fail,
              })
            ),
          ]),
        ],
      } as const);

      expect(machine.returned()).toBe(false);
      machine.tick(
        Emit.event(id, Ev.request, { from: "storage-1", to: "storage-2" })
      );
      expect(machine.returned()).toBe(false);

      machine.tick(Emit.event(id, Ev.reqStorage, { somefield: "somevalue" }));
      expect(machine.returned()).toBe(false);
      expect(machine.state()).toEqual({
        state: [One, Ev.reqStorage],
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });

      machine.tick(Emit.event(id, Ev.bid, {}));

      await sleep(TIMEOUT_DURATION + 100);
      expect(machine.state()).toEqual({
        state: [One, Ev.bid],
        context: { src: "storage-1", dst: "storage-2", somevar: "somevalue" },
      });
    });
  });

  describe("parallel", () => {
    it("produce parallel state and work the next workable code and event", () => {
      const code = Code.make<TheType>();
      const { role } = code.actor;
      const machine = WFMachine<TheType>(
        { id },
        {
          uniqueParams: [],
          code: [
            code.event(role("a"), Ev.request),
            ...code.parallel({ min: 2 }, [code.event(role("a"), Ev.bid)]), // minimum of two bids
            ...code.retry([code.event(role("a"), Ev.accept)]), // test that next code is not immediately event too
          ],
        }
      );

      machine.tick(Emit.event(id, Ev.request, {}));
      expect(machine.state()).toEqual({
        state: [One, Ev.request],
        context: {},
      });

      machine.tick(Emit.event(id, Ev.bid, {}));
      expect(machine.state()).toEqual({
        state: [Parallel, [Ev.bid]],
        context: {},
      });

      machine.tick(Emit.event(id, Ev.accept, {})); // attempt to accept will fail because parallel count isn't fulfilled
      expect(machine.state()).toEqual({
        state: [Parallel, [Ev.bid]],
        context: {},
      });

      machine.tick(Emit.event(id, Ev.bid, {})); // the second bid
      expect(machine.state()).toEqual({
        state: [Parallel, [Ev.bid, Ev.bid]],
        context: {},
      });

      machine.tick(Emit.event(id, Ev.accept, {})); // finally accept should work
      expect(machine.state()).toEqual({
        state: [One, Ev.accept],
        context: {},
      });
    });

    it("works with choice", () => {
      const code = Code.make<TheType>();
      const { role } = code.actor;
      const machine = WFMachine<TheType>(
        { id },
        {
          uniqueParams: [],
          code: [
            code.event(role("a"), Ev.request),
            ...code.parallel({ min: 0 }, [code.event(role("a"), Ev.bid)]), // minimum of two bids
            ...code.choice([
              code.event(role("a"), Ev.accept),
              code.event(role("a"), Ev.deny),
            ]),
          ],
        }
      );

      machine.tick(Emit.event(id, Ev.request, {}));
      machine.tick(Emit.event(id, Ev.deny, {}));
      expect(machine.state()).toEqual({
        state: [One, Ev.deny],
        context: {},
      });
    });

    it("sequence of parallels", () => {
      const code = Code.make<TheType>();
      const { role } = code.actor;
      const machine = WFMachine<TheType>(
        { id },
        {
          uniqueParams: [],
          code: [
            code.event(role("a"), Ev.request),
            ...code.parallel({ min: 2 }, [code.event(role("a"), Ev.bid)]), // minimum of two bids
            ...code.parallel({ min: 2 }, [code.event(role("a"), Ev.accept)]), // minimum of two bids
          ],
        }
      );

      machine.tick(Emit.event(id, Ev.request, {}));
      machine.tick(Emit.event(id, Ev.bid, {}));
      expect(machine.state().state).toEqual([Parallel, [Ev.bid]]);
      machine.tick(Emit.event(id, Ev.bid, {}));
      expect(machine.state().state).toEqual([Parallel, [Ev.bid, Ev.bid]]);
      machine.tick(Emit.event(id, Ev.accept, {}));
      expect(machine.state().state).toEqual([Parallel, [Ev.accept]]);
      machine.tick(Emit.event(id, Ev.accept, {}));
      expect(machine.state().state).toEqual([Parallel, [Ev.accept, Ev.accept]]);
    });

    it("max reached force next", () => {
      const code = Code.make<TheType>();
      const { role } = code.actor;
      const machine = WFMachine<TheType>(
        { id },
        {
          uniqueParams: [],
          code: [
            code.event(role("a"), Ev.request),
            ...code.parallel({ min: 1, max: 2 }, [
              code.event(role("a"), Ev.bid),
            ]), // minimum of two bids
          ],
        }
      );

      machine.tick(Emit.event(id, Ev.request, {}));
      machine.tick(Emit.event(id, Ev.bid, {}));
      expect(machine.state().state).toEqual([Parallel, [Ev.bid]]);
      machine.tick(Emit.event(id, Ev.bid, {}));
      expect(machine.state().state).toEqual([Parallel, [Ev.bid, Ev.bid]]);
      expect(machine.returned()).toBe(true);
    });
  });

  describe("compensation", () => {
    it("passing without compensation", () => {
      const code = Code.make<TheType>();
      const { role } = code.actor;
      const machine = WFMachine<TheType>(
        { id },
        {
          uniqueParams: [],
          code: [
            code.event(role("a"), Ev.inside, {}),
            ...code.compensate(
              [
                code.event(role("a"), Ev.reqLeave),
                code.event(role("a"), Ev.doLeave),
                code.event(role("a"), Ev.success),
              ],
              [
                code.event(role("a"), Ev.withdraw),
                code.event(role("a"), Ev.doLeave),
                code.event(role("a"), Ev.withdrawn),
              ]
            ),
          ],
        }
      );

      machine.tick(Emit.event(id, Ev.inside, {}));
      expect(machine.state().state).toEqual([One, Ev.inside]);

      machine.tick(Emit.event(id, Ev.reqLeave, {}));
      expect(machine.state().state).toEqual([One, Ev.reqLeave]);
      expect(
        machine
          .availableCompensateable()
          .find(
            (x) =>
              x.name === Ev.withdraw &&
              x.actor.t === "Role" &&
              x.actor.get() === "a"
          )
      ).toBeTruthy();

      machine.tick(Emit.event(id, Ev.doLeave, {}));
      expect(machine.state().state).toEqual([One, Ev.doLeave]);
      expect(
        machine
          .availableCompensateable()
          .find(
            (x) =>
              x.name === Ev.withdraw &&
              x.actor.t === "Role" &&
              x.actor.get() === "a"
          )
      ).toBeTruthy();

      machine.tick(Emit.event(id, Ev.success, {}));
      expect(machine.state().state).toEqual([One, Ev.success]);
      expect(machine.availableCompensateable()).toEqual([]);
      expect(machine.returned()).toEqual(true);
    });

    it("passing with compensation", () => {
      const code = Code.make<TheType>();
      const { role } = code.actor;
      const machine = WFMachine<TheType>(
        { id },
        {
          uniqueParams: [],
          code: [
            code.event(role("a"), Ev.inside, {}),
            ...code.compensate(
              [
                code.event(role("a"), Ev.reqLeave),
                code.event(role("a"), Ev.doLeave),
                code.event(role("a"), Ev.success),
              ],
              [
                code.event(role("a"), Ev.withdraw),
                code.event(role("a"), Ev.doLeave),
                code.event(role("a"), Ev.withdrawn),
              ]
            ),
          ],
        }
      );

      machine.tick(Emit.event(id, Ev.inside, {}));
      expect(machine.state().state).toEqual([One, Ev.inside]);

      machine.tick(Emit.event(id, Ev.reqLeave, {}));
      expect(machine.state().state).toEqual([One, Ev.reqLeave]);
      expect(
        machine
          .availableCompensateable()
          .find(
            (x) =>
              x.name === Ev.withdraw &&
              x.actor.t === "Role" &&
              x.actor.get() === "a"
          )
      ).toBeTruthy();

      machine.tick(Emit.event(id, Ev.withdraw, {}));
      expect(machine.state().state).toEqual([One, Ev.withdraw]);
      expect(machine.availableCompensateable()).toEqual([]);
      machine.tick(Emit.event(id, Ev.doLeave, {}));
      expect(machine.state().state).toEqual([One, Ev.doLeave]);
      machine.tick(Emit.event(id, Ev.withdrawn, {}));
      expect(machine.state().state).toEqual([One, Ev.withdrawn]);
      expect(machine.returned()).toEqual(true);
    });
  });

  describe("match", () => {
    it("named match should work", () => {
      const code = Code.make<TheType>();
      const { role } = code.actor;
      const machine = WFMachine<TheType>(
        { id },
        {
          uniqueParams: [],
          code: [
            code.event(role("a"), Ev.request),
            ...code.match(
              {
                uniqueParams: [],
                code: [
                  code.event(role("a"), Ev.inside),
                  ...code.compensate(
                    [
                      code.event(role("a"), Ev.reqLeave),
                      code.event(role("a"), Ev.doLeave),
                      code.event(role("a"), Ev.success),
                    ],
                    [
                      code.event(role("a"), Ev.withdraw),
                      code.event(role("a"), Ev.doLeave),
                      code.event(role("a"), Ev.withdrawn),
                    ]
                  ),
                ],
              },
              {},
              [
                code.matchCase([Exact, Ev.success], []),
                code.matchCase(
                  [Otherwise],
                  [
                    code.event(role("a"), Ev.cancelled, {
                      control: Code.Control.return,
                    }),
                  ]
                ),
              ]
            ),
            code.event(role("a"), Ev.done),
          ],
        }
      );

      machine.tick(Emit.event(id, Ev.request, {}));
      machine.tick(Emit.event(id, Ev.inside, {}));
      machine.tick(Emit.event(id, Ev.reqLeave, {}));
      machine.tick(Emit.event(id, Ev.doLeave, {}));
      machine.tick(Emit.event(id, Ev.success, {}));
      machine.tick(Emit.event(id, Ev.done, {}));
      expect(machine.returned()).toBe(true);
      expect(machine.state().state).toEqual([One, Ev.done]);
    });

    it("otherwise match should work", () => {
      const code = Code.make<TheType>();
      const { role } = code.actor;
      const machine = WFMachine<TheType>(
        { id },
        {
          uniqueParams: [],
          code: [
            code.event(role("a"), Ev.request),
            ...code.match(
              {
                uniqueParams: [],
                code: [
                  code.event(role("a"), Ev.inside),
                  ...code.compensate(
                    [
                      code.event(role("a"), Ev.reqLeave),
                      code.event(role("a"), Ev.doLeave),
                      code.event(role("a"), Ev.success),
                    ],
                    [
                      code.event(role("a"), Ev.withdraw),
                      code.event(role("a"), Ev.doLeave),
                      code.event(role("a"), Ev.withdrawn),
                    ]
                  ),
                ],
              },
              {},
              [
                code.matchCase([Exact, Ev.success], []),
                code.matchCase(
                  [Otherwise],
                  [
                    code.event(role("a"), Ev.cancelled, {
                      control: Code.Control.return,
                    }),
                  ]
                ),
              ]
            ),
            code.event(role("a"), Ev.done),
          ],
        }
      );

      machine.tick(Emit.event(id, Ev.request, {}));
      machine.tick(Emit.event(id, Ev.inside, {}));
      machine.tick(Emit.event(id, Ev.reqLeave, {}));
      machine.tick(Emit.event(id, Ev.doLeave, {}));
      machine.tick(Emit.event(id, Ev.withdraw, {}));
      machine.tick(Emit.event(id, Ev.doLeave, {}));
      machine.tick(Emit.event(id, Ev.withdrawn, {}));
      machine.tick(Emit.event(id, Ev.cancelled, {}));
      expect(machine.returned()).toBe(true);
      expect(machine.state().state).toEqual([One, Ev.cancelled]);
    });
  });

  describe("choice", () => {
    const code = Code.make<TheType>();
    const { role } = code.actor;
    const prepare = () =>
      WFMachine<TheType>(
        { id },
        {
          uniqueParams: [],
          code: [
            code.event(role("a"), Ev.request),
            ...code.choice([
              code.event(role("a"), Ev.accept),
              code.event(role("a"), Ev.deny, { control: Code.Control.return }),
              code.event(role("a"), Ev.assistanceNeeded, {
                control: Code.Control.return,
              }),
            ]),
            code.event(role("a"), Ev.doEnter),
          ],
        }
      );

    it("should work for any event inside the choice - 1", () => {
      const machine = prepare();
      machine.tick(Emit.event(id, Ev.request, {}));
      machine.tick(Emit.event(id, Ev.accept, {}));
      expect(machine.returned()).toEqual(false);
      expect(machine.state().state).toEqual([One, Ev.accept]);
      machine.tick(Emit.event(id, Ev.doEnter, {}));
      expect(machine.returned()).toEqual(true);
      expect(machine.state().state).toEqual([One, Ev.doEnter]);
    });

    it("should work for any event inside the choice - 2", () => {
      const machine = prepare();
      machine.tick(Emit.event(id, Ev.request, {}));
      machine.tick(Emit.event(id, Ev.deny, {}));
      expect(machine.returned()).toEqual(true);
      expect(machine.state().state).toEqual([One, Ev.deny]);
    });

    it("should work for any event inside the choice - 3", () => {
      const machine = prepare();
      machine.tick(Emit.event(id, Ev.request, {}));
      machine.tick(Emit.event(id, Ev.assistanceNeeded, {}));
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
      const { role } = code.actor;
      const machine = WFMachine(
        { id },
        { uniqueParams: [], code: [code.event(role("transporter"), "bid")] }
      );
      expect(
        machine
          .availableCommands()
          .find(
            (x) =>
              x.name === Ev.bid &&
              x.reason === null &&
              x.actor.t === "Role" &&
              x.actor.get() === "transporter"
          )
      ).toBeTruthy();
    });

    it("should work with choice", () => {
      const code = Code.make<MultiRole>();
      const { role } = code.actor;
      const machine = WFMachine(
        { id },
        {
          uniqueParams: [],
          code: [
            code.event(role("transporter"), "bid"),
            ...code.choice([
              code.event(role("storage"), "accept"),
              code.event(role("manager"), "deny"),
            ]),
          ],
        }
      );

      machine.tick(Emit.event(id, Ev.bid, {}));

      expect(
        machine
          .availableCommands()
          .find(
            (x) =>
              x.name === "accept" &&
              x.reason === null &&
              x.actor.t === "Role" &&
              x.actor.get() === "storage"
          )
      ).toBeTruthy();

      expect(
        machine
          .availableCommands()
          .find(
            (x) =>
              x.name === "deny" &&
              x.reason === null &&
              x.actor.t === "Role" &&
              x.actor.get() === "manager"
          )
      ).toBeTruthy();
    });

    it("should work with parallel", () => {
      const code = Code.make<MultiRole>();
      const { role } = code.actor;
      const machine = WFMachine(
        { id },
        {
          uniqueParams: [],
          code: [
            code.event(role("manager"), Ev.request),
            ...code.parallel({ min: 1, max: 2 }, [
              code.event(role("transporter"), Ev.bid),
            ]),
            code.event(role("manager"), Ev.accept),
          ],
        }
      );

      machine.tick(Emit.event(id, Ev.request, {}));

      // min not reached
      expect(
        machine
          .availableCommands()
          .find(
            (x) =>
              x.name === Ev.bid &&
              x.reason === null &&
              x.actor.t === "Role" &&
              x.actor.get() === "transporter"
          )
      ).toBeTruthy();

      expect(
        machine
          .availableCommands()
          .find(
            (x) =>
              x.name === Ev.accept &&
              x.reason === null &&
              x.actor.t === "Role" &&
              x.actor.get() === "manager"
          )
      ).not.toBeTruthy();

      // min reached - maxx not reached
      machine.tick(Emit.event(id, Ev.bid, {}));
      expect(
        machine
          .availableCommands()
          .find(
            (x) =>
              x.name === Ev.bid &&
              x.reason === null &&
              x.actor.t === "Role" &&
              x.actor.get() === "transporter"
          )
      ).toBeTruthy();

      expect(
        machine
          .availableCommands()
          .find(
            (x) =>
              x.name === Ev.accept &&
              x.reason === null &&
              x.actor.t === "Role" &&
              x.actor.get() === "manager"
          )
      ).toBeTruthy();

      // max reached
      machine.tick(Emit.event(id, Ev.bid, {}));
      expect(
        machine
          .availableCommands()
          .find(
            (x) =>
              x.name === Ev.bid &&
              x.reason === null &&
              x.actor.t === "Role" &&
              x.actor.get() === "transporter"
          )
      ).not.toBeTruthy();

      expect(
        machine
          .availableCommands()
          .find(
            (x) =>
              x.name === Ev.accept &&
              x.reason === null &&
              x.actor.t === "Role" &&
              x.actor.get() === "manager"
          )
      ).toBeTruthy();
    });

    it("should work with timeout", async () => {
      const TIMEOUT_DURATION = 100;
      const code = Code.make<MultiRole>();
      const { role } = code.actor;
      const machine = WFMachine(
        { id },
        {
          uniqueParams: [],
          code: [
            code.event(role("manager"), Ev.request, {
              bindings: [],
            }),
            ...code.timeout(
              TIMEOUT_DURATION,
              [
                ...code.parallel({ max: 2 }, [
                  code.event(role("transporter"), Ev.bid),
                ]),
              ],
              code.event(role("manager"), Ev.cancelled, {
                control: Code.Control.fail,
              })
            ),
          ],
        }
      );

      expect(
        machine
          .availableCommands()
          .find(
            (x) =>
              x.name === Ev.request &&
              x.reason === null &&
              x.actor.t === "Role" &&
              x.actor.get() === "manager" &&
              x.control === undefined
          )
      ).toBeTruthy();
      machine.tick(Emit.event(id, Ev.request, {}));

      await sleep(TIMEOUT_DURATION + 1);

      expect(
        machine
          .availableCommands()
          .find(
            (x) =>
              x.name === Ev.bid &&
              x.reason === null &&
              x.actor.t === "Role" &&
              x.actor.get() === "transporter" &&
              x.control === undefined
          )
      ).toBeTruthy();

      expect(
        machine
          .availableCommands()
          .find(
            (x) =>
              x.name === Ev.cancelled &&
              x.reason === "timeout" &&
              x.actor.t === "Role" &&
              x.actor.get() === "manager" &&
              x.control === "fail"
          )
      ).toBeTruthy();
    });

    // // TODO: alan: I'm not clear on how compensation should actually work
    // it("should work with compensation", async () => {
    // });

    it("should work match subworkflow", () => {
      const code = Code.make<MultiRole>();
      const { role } = code.actor;
      const machine = WFMachine<MultiRole>(
        { id },
        {
          uniqueParams: [],
          code: [
            code.event(role("transporter"), Ev.accept),
            ...code.match(
              {
                uniqueParams: [],
                code: [
                  code.event(role("transporter"), Ev.reqEnter),
                  ...code.choice([
                    code.event(role("storage"), Ev.doEnter),
                    code.event(role("transporter"), Ev.withdraw, {
                      control: "return",
                    }),
                  ]),
                  code.event(role("transporter"), Ev.inside),
                ],
              },
              {},
              [
                code.matchCase(
                  [Exact, Ev.inside],
                  [code.event(role("manager"), Ev.success)]
                ),
                code.matchCase(
                  [Otherwise],
                  [code.event(role("manager"), Ev.cancelled)]
                ),
              ]
            ),
          ],
        }
      );

      machine.tick(Emit.event(id, Ev.accept, {}));
      machine.tick(Emit.event(id, Ev.reqEnter, {}));

      expect(
        machine
          .availableCommands()
          .find(
            (x) =>
              x.name === Ev.doEnter &&
              x.reason === null &&
              x.actor.t === "Role" &&
              x.actor.get() === "storage" &&
              x.control === undefined
          )
      ).toBeTruthy();

      expect(
        machine
          .availableCommands()
          .find(
            (x) =>
              x.name === Ev.withdraw &&
              x.reason === null &&
              x.actor.t === "Role" &&
              x.actor.get() === "transporter" &&
              x.control === "return"
          )
      ).toBeTruthy();

      machine.tick(Emit.event(id, Ev.doEnter, {}));
      machine.tick(Emit.event(id, Ev.inside, {}));

      expect(
        machine
          .availableCommands()
          .find(
            (x) =>
              x.name === Ev.success &&
              x.reason === null &&
              x.actor.t === "Role" &&
              x.actor.get() === "manager" &&
              x.control === undefined
          )
      ).toBeTruthy();
    });
  });
});
