import { describe, expect, it } from "@jest/globals";
import {
  ActyxWFBusinessOrMarker,
  extractWFCanonDecideMarker,
  InternalTag,
} from "./consts.js";
import { One, Parallel } from "./wfmachine.js";
import {
  expectAllToHaveSameHistory,
  expectAllToHaveSameState,
  historyOf,
} from "./test-utils/scenario-builder.js";
import { awhile } from "./test-utils/misc.js";
import { Logistics } from "./test-utils/logistic-scenario.js";
import { Choices } from "./test-utils/choices-scenario.js";
import { CanonSwitch } from "./test-utils/canon-switch-scenario.js";

describe("no-partitions", () => {
  const { Ev } = Logistics;

  it("works and build history correctly", async () => {
    const scenario = Logistics.genScenario();
    const { findAndRunCommand } = scenario;
    const { dst, manager, src, t1, t2, t3 } = scenario.agents;

    await findAndRunCommand(manager, Ev.request, {
      from: src.identity.id,
      to: dst.identity.id,
      manager: manager.identity.id,
    });

    expectAllToHaveSameState([manager, src, t1, t2, t3]);

    // assert state at request
    expect(dst.machine.wfmachine().state().state?.[0]).toBe(One);
    expect(dst.machine.wfmachine().state().state?.[1].payload.t).toBe(
      "request"
    );

    await findAndRunCommand(t1, Ev.bid, { bidder: t1.identity.id });
    await findAndRunCommand(t2, Ev.bid, { bidder: t2.identity.id });
    await findAndRunCommand(t3, Ev.bid, { bidder: t3.identity.id });

    // assert base state at request
    // and there are 3 bids in parallel
    expect(dst.machine.wfmachine().state().state?.[0]).toBe(Parallel);
    expect(dst.machine.wfmachine().state().state?.[1].payload.t).toBe(
      "request"
    );
    expect(dst.machine.wfmachine().state().state?.[2]?.length).toBe(3);
    expect(
      dst.machine
        .wfmachine()
        .state()
        .state?.[2]?.find((x) => x.payload.payload.bidder === t1.identity.id)
    ).toBeTruthy();
    expect(
      dst.machine
        .wfmachine()
        .state()
        .state?.[2]?.find((x) => x.payload.payload.bidder === t2.identity.id)
    ).toBeTruthy();
    expect(
      dst.machine
        .wfmachine()
        .state()
        .state?.[2]?.find((x) => x.payload.payload.bidder === t3.identity.id)
    ).toBeTruthy();

    // t1 self assign and accept
    const winner = (() => {
      const state = t1.machine.state().state;
      const winner =
        state &&
        state[0] === Parallel &&
        state[2].find((x) => x.payload.payload.bidder === t1.identity.id)
          ?.payload.payload.bidder;
      if (!winner) throw new Error("no winner");
      return winner;
    })();
    await findAndRunCommand(t1, Ev.assign, { robotID: winner });
    await findAndRunCommand(t1, Ev.accept);

    expect(dst.machine.wfmachine().state().state?.[0]).toBe(One);
    expect(dst.machine.wfmachine().state().state?.[1].payload.t).toBe(
      Ev.accept
    );

    // docking t1 -> src and loading
    await findAndRunCommand(t1, Ev.atSrc);
    await findAndRunCommand(t1, Ev.reqEnter);
    await findAndRunCommand(src, Ev.doEnter);
    await findAndRunCommand(t1, Ev.inside);

    expect(dst.machine.wfmachine().state().state?.[0]).toBe(One);
    expect(dst.machine.wfmachine().state().state?.[1].payload.t).toBe(
      Ev.inside
    );

    await findAndRunCommand(t1, Ev.reqLeave);
    await findAndRunCommand(src, Ev.doLeave);
    await findAndRunCommand(t1, Ev.success);

    // make sure match state is detected from parent state
    expect(dst.machine.wfmachine().state().state?.[1].payload.t).toBe(
      Ev.success
    );

    // load
    await findAndRunCommand(t1, Ev.loaded);

    // docking t1 -> dst and loading
    await findAndRunCommand(t1, Ev.atDst);
    await findAndRunCommand(t1, Ev.reqEnter);
    await findAndRunCommand(dst, Ev.doEnter);
    await findAndRunCommand(t1, Ev.inside);
    await findAndRunCommand(t1, Ev.reqLeave);
    await findAndRunCommand(dst, Ev.doLeave);
    await findAndRunCommand(t1, Ev.success);

    // unload
    await findAndRunCommand(t1, Ev.unloaded);

    // done
    await findAndRunCommand(manager, Ev.done);

    expectAllToHaveSameState([manager, src, t1, t2, t3]);

    expect(historyOf(t1).map((x) => x.payload.t)).toEqual([
      Ev.request,
      // Ev.bid, // parallel is not included in the history chain
      Ev.assign,
      Ev.accept,
      Ev.atSrc,
      Ev.reqEnter,
      Ev.doEnter,
      Ev.inside,
      Ev.reqLeave,
      Ev.doLeave,
      Ev.success,
      Ev.loaded,
      Ev.atDst,
      Ev.reqEnter,
      Ev.doEnter,
      Ev.inside,
      Ev.reqLeave,
      Ev.doLeave,
      Ev.success,
      Ev.unloaded,
      Ev.done,
    ]);
    // validate history in multiverse
  });
});

describe("partitions and compensations", () => {
  const { Ev } = Logistics;
  /**
   * Partitions src and t2 alone
   */
  const scenarioContestingBidOnPartition = async (
    {
      findAndRunCommand,
      network,
      agents: { dst, manager, src, t1, t2, t3 },
    }: Logistics.Scenario,
    whoToPartitions: Logistics.Scenario["agents"][keyof Logistics.Scenario["agents"]][]
  ) => {
    await findAndRunCommand(manager, Ev.request, {
      from: src.identity.id,
      to: dst.identity.id,
      manager: manager.identity.id,
    });

    // partitions
    network.partitions.make(whoToPartitions.map((x) => x.node));

    // both t1 and t2 assign and accepts
    // both will have the same lamport timestamp because of the partition,
    // but t1 will win because of stream id sort
    await findAndRunCommand(t1, Ev.bid, { bidder: t1.identity.id });
    await findAndRunCommand(t1, Ev.assign, {
      robotID: t1.identity.id,
    });
    await findAndRunCommand(t1, Ev.accept);

    await findAndRunCommand(t2, Ev.bid, { bidder: t2.identity.id });
    await findAndRunCommand(t2, Ev.assign, {
      robotID: t2.identity.id,
    });
    await findAndRunCommand(t2, Ev.accept);
  };

  it("converge correctly after partition is closed", async () => {
    const scenario = Logistics.genScenario();
    const {
      findAndRunCommand,
      network,
      agents: { dst, manager, src, t1, t2, t3 },
    } = scenario;

    await scenarioContestingBidOnPartition(scenario, [t2, src]);

    // expect each of t1 and t2 to have their own "reality"
    expect(t1.machine.wfmachine().state().state?.[1].payload.t).toBe(Ev.accept);
    expect(t1.machine.wfmachine().state().context.t).toBe("t1");

    expect(t2.machine.wfmachine().state().state?.[1].payload.t).toBe(Ev.accept);
    expect(t2.machine.wfmachine().state().context.t).toBe("t2");

    expectAllToHaveSameState([manager, t1, t3]);
    expectAllToHaveSameState([src, t2]);

    // after partition, expect same state for all machines
    await network.partitions.connectAll();
    await awhile();
  });

  it("does compensation correctly", async () => {
    const scenario = Logistics.genScenario();
    const {
      findAndRunCommand,
      network,
      agents: { dst, manager, src, t1, t2, t3 },
    } = scenario;

    // isolate t2 and src together
    // t2 and src believes that they should be working together
    await scenarioContestingBidOnPartition(scenario, [t2, src]);

    await findAndRunCommand(t2, Ev.atSrc);
    await findAndRunCommand(t2, Ev.reqEnter);
    await findAndRunCommand(src, Ev.doEnter);

    expect(t1.machine.wfmachine().state().state?.[1].payload.t).toBe(Ev.accept);
    expect(t1.machine.wfmachine().state().context.t).toBe("t1");

    expect(t2.machine.wfmachine().state().state?.[1].payload.t).toBe(
      Ev.doEnter
    );
    expect(t2.machine.wfmachine().state().context.A).toBe("t2");
    expectAllToHaveSameState([t1, manager, dst]);
    expectAllToHaveSameState([t2, src]);

    await network.partitions.connectAll();

    expect(t2.machine.mcomb().t).toBe("off-canon");
    // while src is involved in the partition that produces the compensatable
    // it is not involved in the compensation scheme itself.
    expect(src.machine.mcomb().t).toBe("normal");
    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(dst.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    // compensating
    await findAndRunCommand(t2, Ev.withdrawn);

    // compensation is done
    expect(t2.machine.mcomb().t).toBe("normal");
    expect(src.machine.mcomb().t).toBe("normal");
    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(dst.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    expectAllToHaveSameState([t1, t2, src, manager, dst, t3]);

    // history of t2 and t1 must match regardless of compensations
    expectAllToHaveSameHistory([t1, t2, src, manager, dst, t3]);
  });

  it("does not duplicate WFMarker unnecessarily on machine restarts", async () => {
    const scenario = Logistics.genScenario();
    const {
      findAndRunCommand,
      network,
      agents: { dst, manager, src, t1, t2, t3 },
    } = scenario;

    // isolate t2 and src together
    // t2 and src believes that they should be working together
    await scenarioContestingBidOnPartition(scenario, [t2, src]);

    await findAndRunCommand(t2, Ev.atSrc);
    await findAndRunCommand(t2, Ev.reqEnter);
    await findAndRunCommand(src, Ev.doEnter);

    await network.partitions.connectAll();

    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(src.machine.mcomb().t).toBe("normal");
    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(dst.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    const beforeRestart = t2.node.api.slice();

    await Promise.all(
      [t2, src, t1, manager, dst, t3].map((x) => x.restartMachine())
    );

    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(src.machine.mcomb().t).toBe("normal");
    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(dst.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    const afterRestart = t2.node.api.slice();

    expect(beforeRestart).toEqual(afterRestart);
  });

  it("does nested compensation from the inside first", async () => {
    const scenario = Logistics.genScenario();
    const {
      findAndRunCommand,
      network,
      agents: { dst, manager, src, t1, t2, t3 },
    } = scenario;

    // isolate t2 and src together
    // t2, src, and dst believes that they should be working together
    await scenarioContestingBidOnPartition(scenario, [t2, src, dst]);

    await findAndRunCommand(t2, Ev.atSrc);
    await findAndRunCommand(t2, Ev.reqEnter);
    await findAndRunCommand(src, Ev.doEnter);

    expectAllToHaveSameState([t1, t3, manager]);
    expectAllToHaveSameState([t2, src, dst]);

    await findAndRunCommand(t2, Ev.inside);
    await findAndRunCommand(t2, Ev.reqLeave);
    await findAndRunCommand(src, Ev.doLeave);
    await findAndRunCommand(t2, Ev.success);

    // load
    await findAndRunCommand(t2, Ev.loaded);

    // docking t2 -> dst and loading
    await findAndRunCommand(t2, Ev.atDst);
    await findAndRunCommand(t2, Ev.reqEnter);
    await findAndRunCommand(dst, Ev.doEnter);

    expectAllToHaveSameState([t1, t3, manager]);
    expectAllToHaveSameState([t2, src, dst]);

    await network.partitions.connectAll();

    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(src.machine.mcomb().t).toBe("off-canon");
    // while dst is involved in the partition that produces the compensatable
    // it is not involved in the compensation scheme itself.
    expect(dst.machine.mcomb().t).toBe("normal");

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    await findAndRunCommand(t2, Ev.withdrawn);

    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(src.machine.mcomb().t).toBe("off-canon");
    expect(dst.machine.mcomb().t).toBe("normal");

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    // docking
    await findAndRunCommand(t2, Ev.reqStorage);
    await findAndRunCommand(src, Ev.offerStorage, { storage: src.identity.id });
    await findAndRunCommand(t2, Ev.atWarehouse);

    await findAndRunCommand(t2, Ev.reqEnter);
    await findAndRunCommand(src, Ev.doEnter);
    await findAndRunCommand(t2, Ev.inside);
    await findAndRunCommand(t2, Ev.reqLeave);
    await findAndRunCommand(src, Ev.doLeave);
    await findAndRunCommand(t2, Ev.success);

    await findAndRunCommand(t2, Ev.stashed);

    expect(t2.machine.mcomb().t).toBe("normal");
    expect(src.machine.mcomb().t).toBe("normal");
    expect(dst.machine.mcomb().t).toBe("normal");
    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    expectAllToHaveSameHistory([t1, t2, src, manager, dst, t3]);
  });

  it("is consistent despite changing partitions", async () => {
    const scenario = Logistics.genScenario();
    const {
      findAndRunCommand,
      network,
      agents: { dst, manager, src, t1, t2, t3 },
    } = scenario;

    // isolate t2 and src together
    // t2, src, and dst believes that they should be working together
    await scenarioContestingBidOnPartition(scenario, [t2, src, dst]);

    await findAndRunCommand(t2, Ev.atSrc);
    await findAndRunCommand(t2, Ev.reqEnter);
    await findAndRunCommand(src, Ev.doEnter);

    expectAllToHaveSameState([t1, t3, manager]);
    expectAllToHaveSameState([t2, src, dst]);

    await findAndRunCommand(t2, Ev.inside);
    await findAndRunCommand(t2, Ev.reqLeave);
    await findAndRunCommand(src, Ev.doLeave);
    await findAndRunCommand(t2, Ev.success);

    // load
    await findAndRunCommand(t2, Ev.loaded);

    // docking t2 -> dst and loading
    await findAndRunCommand(t2, Ev.atDst);
    await findAndRunCommand(t2, Ev.reqEnter);
    await findAndRunCommand(dst, Ev.doEnter);

    expectAllToHaveSameState([t1, t3, manager]);
    expectAllToHaveSameState([t2, src, dst]);

    // shuffle through partitions
    await network.partitions.group(
      [t1.node, t3.node, manager.node],
      [src.node, dst.node],
      [t2.node]
    ); // isolate t2
    await network.partitions.group([t2.node]); // isolate t2

    // src and dst now realize they are in a compensation group
    expect(src.machine.mcomb().t).toBe("off-canon");
    // but t2 hasn't yet since it is still partitioned
    expect(t2.machine.mcomb().t).toBe("normal");
    // dst was in the partition that creates the compensateable but it is not
    // necessary for the compensation, therefore it is normal instead of
    // off-canon
    expect(dst.machine.mcomb().t).toBe("normal");

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    await network.partitions.connectAll();

    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(src.machine.mcomb().t).toBe("off-canon");
    expect(dst.machine.mcomb().t).toBe("normal");

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    await findAndRunCommand(t2, Ev.withdrawn);

    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(src.machine.mcomb().t).toBe("off-canon");
    expect(dst.machine.mcomb().t).toBe("normal");

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    // docking
    await findAndRunCommand(t2, Ev.reqStorage);
    await findAndRunCommand(src, Ev.offerStorage, { storage: src.identity.id });
    await findAndRunCommand(t2, Ev.atWarehouse);

    await findAndRunCommand(t2, Ev.reqEnter);
    await findAndRunCommand(src, Ev.doEnter);
    await findAndRunCommand(t2, Ev.inside);
    await findAndRunCommand(t2, Ev.reqLeave);
    await findAndRunCommand(src, Ev.doLeave);
    await findAndRunCommand(t2, Ev.success);

    await findAndRunCommand(t2, Ev.stashed);

    expect(t2.machine.mcomb().t).toBe("normal");
    expect(src.machine.mcomb().t).toBe("normal");
    expect(dst.machine.mcomb().t).toBe("normal");
    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    expectAllToHaveSameHistory([t1, t2, src, manager, dst, t3]);
  });

  // prettier-ignore
  const comp_history = ([{"meta":{"offset":0,"appId":"","eventId":"eid:0","isLocalEvent":true,"lamport":0,"stream":"manager","tags":["workflowtag","ax:wf:by:manager"],"timestampMicros":1723208607236000},"payload":{"t":"request","payload":{"from":"storage-src","to":"storage-dst","manager":"manager"}}},{"meta":{"offset":0,"appId":"","eventId":"eid:1","isLocalEvent":true,"lamport":1,"stream":"t1","tags":["workflowtag","ax:wf:predecessor:eid:0","ax:wf:by:t1"],"timestampMicros":1723208607239000},"payload":{"t":"bid","payload":{"bidder":"t1"}}},{"meta":{"offset":0,"appId":"","eventId":"eid:4","isLocalEvent":true,"lamport":1,"stream":"t2","tags":["workflowtag","ax:wf:predecessor:eid:0","ax:wf:by:t2"],"timestampMicros":1723208607244000},"payload":{"t":"bid","payload":{"bidder":"t2"}}},{"meta":{"offset":1,"appId":"","eventId":"eid:2","isLocalEvent":true,"lamport":2,"stream":"t1","tags":["workflowtag","ax:wf:predecessor:eid:0","ax:wf:by:t1"],"timestampMicros":1723208607241000},"payload":{"t":"assign","payload":{"robotID":"t1"}}},{"meta":{"offset":1,"appId":"","eventId":"eid:5","isLocalEvent":true,"lamport":2,"stream":"t2","tags":["workflowtag","ax:wf:predecessor:eid:0","ax:wf:by:t2"],"timestampMicros":1723208607245000},"payload":{"t":"assign","payload":{"robotID":"t2"}}},{"meta":{"offset":2,"appId":"","eventId":"eid:3","isLocalEvent":true,"lamport":3,"stream":"t1","tags":["workflowtag","ax:wf:predecessor:eid:2","ax:wf:by:t1"],"timestampMicros":1723208607243000},"payload":{"t":"accept","payload":{}}},{"meta":{"offset":2,"appId":"","eventId":"eid:6","isLocalEvent":true,"lamport":3,"stream":"t2","tags":["workflowtag","ax:wf:predecessor:eid:5","ax:wf:by:t2"],"timestampMicros":1723208607246000},"payload":{"t":"accept","payload":{}}},{"meta":{"offset":3,"appId":"","eventId":"eid:7","isLocalEvent":true,"lamport":4,"stream":"t2","tags":["workflowtag","ax:wf:predecessor:eid:6","ax:wf:by:t2"],"timestampMicros":1723208607247000},"payload":{"t":"atSrc","payload":{}}},{"meta":{"offset":4,"appId":"","eventId":"eid:8","isLocalEvent":true,"lamport":5,"stream":"t2","tags":["workflowtag","ax:wf:predecessor:eid:7","ax:wf:by:t2"],"timestampMicros":1723208607248000},"payload":{"t":"reqEnter","payload":{}}},{"meta":{"offset":0,"appId":"","eventId":"eid:9","isLocalEvent":true,"lamport":6,"stream":"storage-src","tags":["workflowtag","ax:wf:predecessor:eid:8","ax:wf:by:storage-src"],"timestampMicros":1723208607250000},"payload":{"t":"doEnter","payload":{}}},{"meta":{"offset":1,"appId":"","eventId":"eid:10","isLocalEvent":true,"lamport":7,"stream":"storage-src","tags":["workflowtag"],"timestampMicros":1723208607261000},"payload":{"ax":"ax:wf:compensation:needed:","actor":"storage-src","fromTimelineOf":"eid:8","toTimelineOf":"eid:3","codeIndex":[17,1]}},{"meta":{"offset":5,"appId":"","eventId":"eid:11","isLocalEvent":true,"lamport":8,"stream":"t2","tags":["workflowtag"],"timestampMicros":1723208607264000},"payload":{"ax":"ax:wf:compensation:needed:","actor":"t2","fromTimelineOf":"eid:8","toTimelineOf":"eid:3","codeIndex":[17,1]}}])
    .map((entry) => {
      const date = new Date(Math.round(entry.meta.timestampMicros / 1000))
      return ({
        meta: ({
          ...entry.meta,
          timestampAsDate: () => date,
        }),
        payload: entry.payload
      }) as ActyxWFBusinessOrMarker<Logistics.CType>
    })

  it("remembers compensation", async () => {
    // history is loaded that triggers compensation in t2 and src (the above
    // scenario before Ev.withdrawn is called)
    const scenario = Logistics.genScenario({
      initialStoreData: comp_history,
    });
    const {
      findAndRunCommand,
      network,
      agents: { dst, manager, src, t1, t2, t3 },
    } = scenario;

    await awhile();

    // t2 and src is in doEnter
    [t2, src].forEach((m) =>
      expect(m.machine.wfmachine().state()?.state?.[1].payload.t).toBe(
        Ev.doEnter
      )
    );

    [t1, manager, dst, t3].forEach((m) =>
      expect(m.machine.wfmachine().state()?.state?.[1].payload.t).toBe(
        Ev.accept
      )
    );

    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(src.machine.mcomb().t).toBe("off-canon");

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(dst.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    // t2 compensating, src follows
    await findAndRunCommand(t2, Ev.withdrawn);

    // compensation is done
    expect(t2.machine.mcomb().t).toBe("normal");
    expect(src.machine.mcomb().t).toBe("normal");

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(dst.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    expectAllToHaveSameState([t1, t2, src, manager, dst, t3]);

    expectAllToHaveSameHistory([t1, t2, src, manager, dst, t3]);
  });

  it("remembers nested compensation", async () => {
    const scenario = Choices.genScenario();
    const {
      findAndRunCommand,
      network,
      agents: { client, t1, t2, t3 },
    } = scenario;
    const { Ev: Evs } = Choices;

    await findAndRunCommand(client, Evs.start);
    await network.partitions.group(
      [client.node],
      [t1.node],
      [t2.node],
      [t3.node]
    );

    // everyone bids themselves to L2
    await Promise.all(
      [t1, t2, t3].map(async (t) => {
        await findAndRunCommand(t, Evs.L1Bid, { bidder: t.identity.id });
        await findAndRunCommand(t, Evs.L1Accept, { assignee: t.identity.id });
        await findAndRunCommand(t, Evs.L2Bid, { bidder: t.identity.id });
        await findAndRunCommand(t, Evs.L2Accept, { assignee: t.identity.id });
      })
    );

    await Promise.all([t1, t2, t3].map((x) => x.restartMachine()));
    // trigger compensation in t3
    await network.partitions.group(
      [client.node],
      [t1.node],
      [t2.node, t3.node]
    );
    await Promise.all([t1, t2, t3].map((x) => x.restartMachine()));

    await network.partitions.connectAll();
    await Promise.all([t1, t2, t3].map((x) => x.restartMachine()));

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(t3.machine.mcomb().t).toBe("off-canon");

    await findAndRunCommand(t2, Evs.L2Compensate);
    await findAndRunCommand(t2, Evs.L1Compensate);
    await findAndRunCommand(t3, Evs.L2Compensate);
    await findAndRunCommand(t3, Evs.L1Compensate);

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(t2.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");
  });
});

describe("timeout", () => {
  const { Ev } = Logistics;

  it("failure consequence loops to retry", async () => {
    const scenario = Logistics.genScenario();
    const { findAndRunCommand } = scenario;
    const { dst, manager, src, t1, t2, t3 } = scenario.agents;

    await findAndRunCommand(manager, Ev.request, {
      from: src.identity.id,
      to: dst.identity.id,
      manager: manager.identity.id,
    });
    await findAndRunCommand(t1, Ev.bid, { bidder: t1.identity.id });
    await findAndRunCommand(t1, Ev.assign, {
      robotID: t1.identity.id,
    });
    await findAndRunCommand(manager, Ev.notAccepted, {});

    expect(manager.machine.state().state?.[1].payload.t).toBe(Ev.notAccepted);

    [t1, t2, t3].forEach((m) => {
      const bidCommand = m.machine
        .commands()
        .find((x) => x.info.name === Ev.bid);
      expect(bidCommand).toBeTruthy();
    });
    await findAndRunCommand(t1, Ev.bid, { bidder: t1.identity.id });
    await findAndRunCommand(t1, Ev.assign, {
      robotID: t1.identity.id,
    });
    await findAndRunCommand(t1, Ev.accept);

    // the history also loops
    expect(historyOf(t1).map((x) => x.payload.t)).toEqual([
      Ev.request,
      Ev.assign,
      Ev.notAccepted,
      Ev.assign,
      Ev.accept,
    ]);
  });

  it("invocation clears the compensations and timeouts", async () => {
    const scenario = Logistics.genScenario();
    const { findAndRunCommand } = scenario;
    const { dst, manager, src, t1, t2, t3 } = scenario.agents;

    await findAndRunCommand(manager, Ev.request, {
      from: src.identity.id,
      to: dst.identity.id,
      manager: manager.identity.id,
    });
    await findAndRunCommand(t1, Ev.bid, { bidder: t1.identity.id });
    await findAndRunCommand(t2, Ev.bid, { bidder: t2.identity.id });
    await findAndRunCommand(t3, Ev.bid, { bidder: t3.identity.id });

    // t1 self assign and accept
    const winner = (() => {
      const state = t1.machine.state().state;
      const winner =
        state &&
        state[0] === Parallel &&
        state[2].find((x) => x.payload.payload.bidder === t1.identity.id)
          ?.payload.payload.bidder;
      if (!winner) throw new Error("no winner");
      return winner;
    })();
    await findAndRunCommand(t1, Ev.assign, { robotID: winner });

    await findAndRunCommand(t1, Ev.accept);

    // docking t1 -> src and loading
    await findAndRunCommand(t1, Ev.atSrc);
    await findAndRunCommand(t1, Ev.reqEnter);
    await findAndRunCommand(src, Ev.doEnter);
    await findAndRunCommand(t1, Ev.inside);

    await findAndRunCommand(t1, Ev.reqLeave);
    await findAndRunCommand(src, Ev.doLeave);
    await findAndRunCommand(t1, Ev.success);

    // load
    await findAndRunCommand(t1, Ev.loaded);

    // docking t1 -> dst and loading
    await findAndRunCommand(t1, Ev.atDst);
    await findAndRunCommand(t1, Ev.reqEnter);

    // done
    await findAndRunCommand(manager, Ev.logisticFailed);

    expectAllToHaveSameState([manager, src, t1, t2, t3, dst]);

    [manager, src, t1, t2, t3, dst].forEach((m) => {
      expect(m.machine.mcomb().t).toBe("normal");
      expect(m.machine.wfmachine().availableCompensateable().length).toBe(0);
      expect(m.machine.wfmachine().availableTimeouts().length).toBe(0);
    });

    expect(manager.machine.wfmachine().state().state?.[1].payload.t).toBe(
      Ev.logisticFailed
    );
  });
});

describe("retry-fail inside compensation", () => {
  const { Ev } = Logistics;

  it("should clear active compensation", async () => {
    const scenario = Logistics.genScenario();
    const { findAndRunCommand } = scenario;
    const { dst, manager, src, t1, t2, t3 } = scenario.agents;
    await findAndRunCommand(manager, Ev.request, {
      from: src.identity.id,
      to: dst.identity.id,
      manager: manager.identity.id,
    });

    await findAndRunCommand(t1, Ev.bid, { bidder: t1.identity.id });
    await findAndRunCommand(t2, Ev.bid, { bidder: t2.identity.id });
    await findAndRunCommand(t3, Ev.bid, { bidder: t3.identity.id });

    // t1 self assign and accept
    const winner = (() => {
      const state = t1.machine.state().state;
      const winner =
        state &&
        state[0] === Parallel &&
        state[2].find((x) => x.payload.payload.bidder === t1.identity.id)
          ?.payload.payload.bidder;
      if (!winner) throw new Error("no winner");
      return winner;
    })();
    await findAndRunCommand(t1, Ev.assign, { robotID: winner });
    await findAndRunCommand(t1, Ev.accept);
    // docking t1 -> src and loading
    await findAndRunCommand(t1, Ev.atSrc);
    await findAndRunCommand(t1, Ev.reqEnter);
    await findAndRunCommand(src, Ev.deny);

    await findAndRunCommand(t1, Ev.notPickedUp);

    expect(manager.machine.wfmachine().state().state?.[1].payload.t).toBe(
      Ev.notPickedUp
    );
    expectAllToHaveSameState([manager, src, t1, t2, t3, dst]);

    // bid command is available again for t1,t2,and t3
    [t1, t2, t3].forEach((x) => {
      const bidCommands = x.machine
        .commands()
        .find((x) => x.info.name === "bid");
      expect(bidCommands).toBeTruthy();
    });

    // history test
    expect(historyOf(t1).map((x) => x.payload.t)).toEqual([
      Ev.request,
      Ev.assign,
      Ev.accept,
      Ev.atSrc,
      Ev.reqEnter,
      Ev.deny,
      Ev.notPickedUp,
    ]);
  });
});

describe("canonization", () => {
  it("switches the canon timeline for the entire swarm", async () => {
    const scenario = CanonSwitch.genScenario();
    const {
      findAndRunCommand,
      network,
      agents: { authoritative1, authoritative2, t1, t2, t3 },
    } = scenario;
    const { Ev: Evs } = CanonSwitch;

    await findAndRunCommand(authoritative1, Evs.start);
    await findAndRunCommand(authoritative1, Evs.assign, {
      canonizer: authoritative1.node.id,
    });
    await network.partitions.group(
      [authoritative1.node, authoritative2.node],
      [t1.node],
      [t2.node],
      [t3.node]
    );

    // everyone bids themselves to L2
    await Promise.all(
      [t1, t2, t3].map(async (t) => {
        await findAndRunCommand(t, Evs.L1Bid, { bidder: t.identity.id });
        await findAndRunCommand(t, Evs.L1Accept, { assignee: t.identity.id });
      })
    );

    await findAndRunCommand(t2, Evs.L1FirstAdvertise);

    // join t1, t2, t3
    // isolate canonizer.node, delaying canonization
    await network.partitions.group([authoritative1.node, authoritative2.node]);

    // t1 is canon now but t2 has no compensations available while off-canon
    // because it is waiting for canonization.
    expect(t1.machine.mcomb().t).toBe("normal");
    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(t3.machine.mcomb().t).toBe("off-canon");

    const t2Comps = t2.machine
      .commands()
      .filter((x) => x.info.reason.has("compensation"));
    expect(t2Comps.length).toBe(0);

    // "canonizer" is in the group now.
    // It will automatically canonize t2 because it's in the season.
    await network.partitions.connectAll();

    // now t2 is canon, t1 and t3 is off-canon
    expect(t1.machine.mcomb().t).toBe("off-canon");
    expect(t2.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("off-canon");

    expect(t2.machine.state().state?.[1].payload.t).toBe(Evs.L1FirstAdvertise);

    await findAndRunCommand(t1, Evs.L1Compensate);
    await findAndRunCommand(t3, Evs.L1Compensate);

    expectAllToHaveSameState([t1, t2, t3, authoritative1, authoritative2]);
  });

  it("ignores late canonization advertisement", async () => {
    const scenario = CanonSwitch.genScenario();
    const {
      findAndRunCommand,
      network,
      agents: { authoritative1, authoritative2, t1, t2, t3 },
    } = scenario;
    const { Ev: Evs } = CanonSwitch;

    await findAndRunCommand(authoritative1, Evs.start);
    await findAndRunCommand(authoritative1, Evs.assign, {
      canonizer: authoritative1.node.id,
    });
    await network.partitions.group(
      [authoritative1.node, authoritative2.node],
      [t1.node],
      [t2.node],
      [t3.node]
    );

    // everyone bids themselves to L2
    await Promise.all(
      [t1, t2, t3].map(async (t) => {
        await findAndRunCommand(t, Evs.L1Bid, { bidder: t.identity.id });
        await findAndRunCommand(t, Evs.L1Accept, { assignee: t.identity.id });
      })
    );

    await findAndRunCommand(t2, Evs.L1FirstAdvertise);

    // isolate canonizer.node, delaying canonization
    await network.partitions.group([authoritative1.node, authoritative2.node]);

    // t1 is canon now
    expect(t1.machine.mcomb().t).toBe("normal");
    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(t3.machine.mcomb().t).toBe("off-canon");

    // canonizer joins the group. t1 is isolated
    await network.partitions.group([t1.node]);

    // t1 still thinks it is normal
    expect(t1.machine.mcomb().t).toBe("normal");
    // t2 and t3 agrees with each other
    expect(t2.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("off-canon");
    await findAndRunCommand(t3, Evs.L1Compensate);

    // TODO: here canonizer is stuck with t1's compensation state to release
    // canonizer from this state where it is actually not-involved, we need
    // involvement tracking mechanism
    // expectAllToHaveSameState([canonizer, t2, t3]);
    expectAllToHaveSameState([t2, t3]);

    // t1 triggers a state where it is advertised
    await findAndRunCommand(t1, Evs.L1FirstAdvertise);

    // everyone is in the group now.
    await network.partitions.connectAll();

    // despite t1's advertisement being out, its name has been decided before t1 joined
    // therefore the whole swarm now agrees that t2 is the winner.
    // t1's advertisement was late in the eye of canonizer
    expect(t1.machine.mcomb().t).toBe("off-canon");

    await findAndRunCommand(t1, Evs.L1Compensate);

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(t2.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");
    expect(authoritative1.machine.mcomb().t).toBe("normal");
    expect(authoritative2.machine.mcomb().t).toBe("normal");
    expectAllToHaveSameState([t1, t2, t3, authoritative1, authoritative2]);
  });

  it("works well with loop", async () => {
    const scenario = CanonSwitch.genScenario();
    const {
      findAndRunCommand,
      network,
      agents: { authoritative1, authoritative2, t1, t2, t3 },
    } = scenario;
    const { Ev: Evs } = CanonSwitch;

    await findAndRunCommand(authoritative1, Evs.start);
    await findAndRunCommand(authoritative1, Evs.assign, {
      canonizer: authoritative1.node.id,
    });
    await network.partitions.group(
      [authoritative1.node, authoritative2.node],
      [t1.node],
      [t2.node],
      [t3.node]
    );

    // everyone bids themselves to L2
    await Promise.all(
      [t1, t2, t3].map(async (t) => {
        await findAndRunCommand(t, Evs.L1Bid, { bidder: t.identity.id });
        await findAndRunCommand(t, Evs.L1Accept, { assignee: t.identity.id });
      })
    );
    // clear canonize for t2
    await network.partitions.group([
      authoritative1.node,
      authoritative2.node,
      t2.node,
    ]);

    await network.partitions.group([authoritative1.node, t2.node]);
    await findAndRunCommand(t2, Evs.L1FirstAdvertise);
    await findAndRunCommand(t2, Evs.L1TriggerLoop); // loop here once
    await findAndRunCommand(t2, Evs.L1FirstAdvertise);
    await findAndRunCommand(t2, Evs.L1SecondAdvertise);

    await network.partitions.connectAll();

    expect(t1.machine.mcomb().t).toBe("off-canon");
    // t2 and t3 agrees with each other
    expect(t2.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("off-canon");

    // there should be 2 decisions for t2's `Evs.L1FirstAdvertise` on both iteration of the loop
    const decisions = extractWFCanonDecideMarker(t2.node.api.slice()).filter(
      (x) => {
        const nameMatches = x.payload.name === Evs.L1FirstAdvertise;
        const actorIsT2 =
          (
            t2.machine.multiverseTree().getById(x.payload.timelineOf)?.meta
              .tags || []
          )
            .map((x) => InternalTag.ActorWriter.read(x))
            .findIndex((x) => x === t2.identity.id) !== -1;

        return nameMatches && actorIsT2;
      }
    );
    expect(decisions.length).toBe(2);

    const uniqueDepth = new Set<number>();
    decisions.forEach((dec) => uniqueDepth.add(dec.payload.depth));

    expect(uniqueDepth.size).toBe(2);
  });

  describe("competition", () => {
    it("chooses the greater decision in 2-way competition", async () => {
      const scenario = CanonSwitch.genScenario();
      const {
        findAndRunCommand,
        network,
        agents: { authoritative1, authoritative2, t1, t2, t3 },
      } = scenario;
      const { Ev: Evs } = CanonSwitch;

      await findAndRunCommand(authoritative1, Evs.start);

      // partition into 2 groups and make each of them have canonizers
      await network.partitions.group(
        [authoritative1.node, t1.node],
        [authoritative2.node, t2.node]
      );

      // each partition canonize their branch
      await Promise.all(
        [[authoritative1, t1] as const, [authoritative2, t2] as const].map(
          async ([canonizer, t]) => {
            await findAndRunCommand(canonizer, Evs.assign, {
              canonizer: canonizer.node.id,
            });
            await findAndRunCommand(t, Evs.L1Bid, { bidder: t.identity.id });
            await findAndRunCommand(t, Evs.L1Accept, {
              assignee: t.identity.id,
            });
            await findAndRunCommand(t, Evs.L1FirstAdvertise);
          }
        )
      );

      expect(t1.machine.wfmachine().latestStateEvent()?.payload.t).toBe(
        Evs.L1FirstAdvertise
      );
      expect(t2.machine.wfmachine().latestStateEvent()?.payload.t).toBe(
        Evs.L1FirstAdvertise
      );

      // clear the partition
      await network.partitions.connectAll();

      // Greater wins: authoritative2 wins because its "streamId" is "authoritative2", greater than "authoritative1"
      expect(t1.machine.mcomb().t).toBe("off-canon");
      expect(t2.machine.mcomb().t).toBe("normal");
    });

    // NOTE: does canonization competition breaks compensation? I don't think
    // so. A compensation can only be broken if somehow the compensation's
    // branch is revalidated again, which is not possible even with canonization
    // competition.
    // Compensation tracking works well with canonization by relying to the fact
    // that there is no "canonize back to the decanonized branch". Compensation
    // tracking relies on the actor having been in the state where it has "more
    // compensateable" to "less compensateable" as compensations negates those
    // compensateable.

    it("chooses the greater decision in 3-way competition", async () => {
      const scenario = CanonSwitch.genScenario();
      const {
        findAndRunCommand,
        network,
        agents: { authoritative1, authoritative2, authoritative3, t1, t2, t3 },
      } = scenario;
      const { Ev: Evs } = CanonSwitch;

      await findAndRunCommand(authoritative1, Evs.start);

      const groups = [
        [authoritative1, t1] as const,
        [authoritative2, t2] as const,
        [authoritative3, t3] as const,
      ];

      // partition into 3 groups and make each of them have canonizers
      await network.partitions.group(
        ...groups.map((group) => {
          return group.map((agent) => {
            return agent.node;
          });
        })
      );

      // each partition canonize their branch
      await Promise.all(
        groups.map(async ([canonizer, t]) => {
          await findAndRunCommand(canonizer, Evs.assign, {
            canonizer: canonizer.node.id,
          });
          await findAndRunCommand(t, Evs.L1Bid, { bidder: t.identity.id });
          await findAndRunCommand(t, Evs.L1Accept, { assignee: t.identity.id });
          await findAndRunCommand(t, Evs.L1FirstAdvertise);
        })
      );

      // supposedly: authoritative3 > authoritative2 > authoritative1

      // join group 1 and group 3
      await network.partitions.group(
        [authoritative1.node, t1.node, authoritative2.node, t2.node],
        [authoritative3.node, t3.node]
      );

      expect(t1.machine.mcomb().t).toBe("off-canon");
      expect(t2.machine.mcomb().t).toBe("normal");

      // t1 should be compensating now
      expect(t1.machine.compensation()).not.toBe(null);

      // connect all now, here will be a competing canonization
      await network.partitions.connectAll();

      expect(t1.machine.compensation()).not.toBe(null);
      expect(t2.machine.compensation()).not.toBe(null);

      await findAndRunCommand(t1, Evs.L1Compensate);
      await findAndRunCommand(t2, Evs.L1Compensate);

      expect(t1.machine.mcomb().t).toBe("normal");
      expect(t2.machine.mcomb().t).toBe("normal");
      expect(t3.machine.mcomb().t).toBe("normal");

      expectAllToHaveSameState([
        t1,
        t2,
        t3,
        authoritative1,
        authoritative2,
        authoritative3,
      ]);
    });
  });
});
