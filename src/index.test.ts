import { describe, expect, it } from "@jest/globals";
import { ActyxWFBusinessOrMarker } from "./consts.js";
import { One, Parallel } from "./wfmachine.js";
import {
  expectAllToHaveSameHistory,
  expectAllToHaveSameState,
  historyOf,
} from "./test-utils/scenario-builder.js";
import { awhile, log } from "./test-utils/misc.js";
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
    await network.partitions.clear();
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

    await network.partitions.clear();

    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(src.machine.mcomb().t).toBe("off-canon");

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

    await network.partitions.clear();

    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(src.machine.mcomb().t).toBe("off-canon");

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(dst.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    const beforeRestart = t2.node.api.slice();

    await Promise.all(
      [t2, src, t1, manager, dst, t3].map((x) => x.restartMachine())
    );

    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(src.machine.mcomb().t).toBe("off-canon");

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

    await network.partitions.clear();

    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(src.machine.mcomb().t).toBe("off-canon");
    expect(dst.machine.mcomb().t).toBe("off-canon");

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    await findAndRunCommand(t2, Ev.withdrawn);

    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(src.machine.mcomb().t).toBe("off-canon");
    expect(dst.machine.mcomb().t).toBe("off-canon");

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
    expect(dst.machine.mcomb().t).toBe("off-canon");
    // but t2 hasn't yet since it is still partitioned
    expect(t2.machine.mcomb().t).toBe("normal");

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    await network.partitions.clear();

    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(src.machine.mcomb().t).toBe("off-canon");
    expect(dst.machine.mcomb().t).toBe("off-canon");

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(manager.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("normal");

    await findAndRunCommand(t2, Ev.withdrawn);

    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(src.machine.mcomb().t).toBe("off-canon");
    expect(dst.machine.mcomb().t).toBe("off-canon");

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
  const comp_history = ([{"meta":{"offset":0,"appId":"","eventId":"6dc3039e-2894-4914-ae4a-9d8688c9ff6d","isLocalEvent":true,"lamport":0,"stream":"manager","tags":["workflowtag"],"timestampMicros":1720698809630000},"payload":{"t":"request","payload":{"from":"storage-src","to":"storage-dst","manager":"manager"}}},{"meta":{"offset":0,"appId":"","eventId":"0b05a57a-ecc5-42dc-90a5-da74206c5935","isLocalEvent":true,"lamport":6,"stream":"storage-src","tags":["workflowtag","ax:wf:predecessor:07528c25-bfef-4a17-8861-1d3cbe0a1bdf"],"timestampMicros":1720698809635000},"payload":{"t":"doEnter","payload":{}}},{"meta":{"offset":1,"appId":"","eventId":"181b0e65-642a-436b-b542-d1e7911deea3","isLocalEvent":true,"lamport":7,"stream":"storage-src","tags":["workflowtag"],"timestampMicros":1720698809649000},"payload":{"ax":"ax:wf:compensation:needed:","actor":"storage-src","fromTimelineOf":"07528c25-bfef-4a17-8861-1d3cbe0a1bdf","toTimelineOf":"9967f952-db85-493c-b5f0-2ee1fcfe2989","codeIndex":[13,1]}},{"meta":{"offset":0,"appId":"","eventId":"349059c3-62f0-475d-bd61-010caf680b0c","isLocalEvent":true,"lamport":1,"stream":"t1","tags":["workflowtag","ax:wf:predecessor:6dc3039e-2894-4914-ae4a-9d8688c9ff6d"],"timestampMicros":1720698809631000},"payload":{"t":"bid","payload":{"bidder":"t1"}}},{"meta":{"offset":1,"appId":"","eventId":"07126407-1274-4b3c-8087-1f7ee57c0821","isLocalEvent":true,"lamport":2,"stream":"t1","tags":["workflowtag","ax:wf:predecessor:6dc3039e-2894-4914-ae4a-9d8688c9ff6d"],"timestampMicros":1720698809632000},"payload":{"t":"assign","payload":{"robotID":"t1"}}},{"meta":{"offset":2,"appId":"","eventId":"9967f952-db85-493c-b5f0-2ee1fcfe2989","isLocalEvent":true,"lamport":3,"stream":"t1","tags":["workflowtag","ax:wf:predecessor:07126407-1274-4b3c-8087-1f7ee57c0821"],"timestampMicros":1720698809632000},"payload":{"t":"accept","payload":{}}},{"meta":{"offset":0,"appId":"","eventId":"b183f4b8-8a66-455b-ac0c-688dd52b5d78","isLocalEvent":true,"lamport":1,"stream":"t2","tags":["workflowtag","ax:wf:predecessor:6dc3039e-2894-4914-ae4a-9d8688c9ff6d"],"timestampMicros":1720698809633000},"payload":{"t":"bid","payload":{"bidder":"t2"}}},{"meta":{"offset":1,"appId":"","eventId":"802bd3c7-36c1-4d52-9179-936f8fbdcdee","isLocalEvent":true,"lamport":2,"stream":"t2","tags":["workflowtag","ax:wf:predecessor:6dc3039e-2894-4914-ae4a-9d8688c9ff6d"],"timestampMicros":1720698809633000},"payload":{"t":"assign","payload":{"robotID":"t2"}}},{"meta":{"offset":2,"appId":"","eventId":"2586ee84-1c57-4866-9c9c-225915a62adf","isLocalEvent":true,"lamport":3,"stream":"t2","tags":["workflowtag","ax:wf:predecessor:802bd3c7-36c1-4d52-9179-936f8fbdcdee"],"timestampMicros":1720698809633000},"payload":{"t":"accept","payload":{}}},{"meta":{"offset":3,"appId":"","eventId":"f82d80ba-cecb-4f52-be7e-48fce4404872","isLocalEvent":true,"lamport":4,"stream":"t2","tags":["workflowtag","ax:wf:predecessor:2586ee84-1c57-4866-9c9c-225915a62adf"],"timestampMicros":1720698809634000},"payload":{"t":"atSrc","payload":{}}},{"meta":{"offset":4,"appId":"","eventId":"07528c25-bfef-4a17-8861-1d3cbe0a1bdf","isLocalEvent":true,"lamport":5,"stream":"t2","tags":["workflowtag","ax:wf:predecessor:f82d80ba-cecb-4f52-be7e-48fce4404872"],"timestampMicros":1720698809634000},"payload":{"t":"reqEnter","payload":{}}},{"meta":{"offset":5,"appId":"","eventId":"b7d342ef-cedf-44b5-a601-72aa168d50f5","isLocalEvent":true,"lamport":8,"stream":"t2","tags":["workflowtag"],"timestampMicros":1720698809652000},"payload":{"ax":"ax:wf:compensation:needed:","actor":"t2","fromTimelineOf":"07528c25-bfef-4a17-8861-1d3cbe0a1bdf","toTimelineOf":"9967f952-db85-493c-b5f0-2ee1fcfe2989","codeIndex":[13,1]}}])
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

    await network.partitions.clear();
    await Promise.all([t1, t2, t3].map((x) => x.restartMachine()));

    expect(t1.machine.mcomb().t).toBe("normal");
    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(t3.machine.mcomb().t).toBe("off-canon");

    await findAndRunCommand(t3, Evs.L2Compensate);
    await findAndRunCommand(t2, Evs.L1Compensate);

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
  it("works", async () => {
    const scenario = CanonSwitch.genScenario();
    const {
      findAndRunCanonization,
      findAndRunCommand,
      network,
      agents: { canonizer, t1, t2, t3 },
    } = scenario;
    const { Ev: Evs } = CanonSwitch;

    await findAndRunCommand(canonizer, Evs.start, {
      canonizer: canonizer.node.id,
    });
    await network.partitions.group(
      [canonizer.node],
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

    await findAndRunCommand(t2, Evs.L1Start);

    // trigger compensation in t3
    await network.partitions.clear();

    // t1 is canon now
    expect(t1.machine.mcomb().t).toBe("normal");
    expect(t2.machine.mcomb().t).toBe("off-canon");
    expect(t3.machine.mcomb().t).toBe("off-canon");

    const t2Comps = t2.machine
      .commands()
      .filter((x) => x.info.reason.has("compensation"));
    const t3Comps = t3.machine
      .commands()
      .filter((x) => x.info.reason.has("compensation"));

    expect(t2Comps.length).toBe(0);
    expect(t3Comps.length).not.toBe(0);

    // canonizer canonize t2
    await findAndRunCanonization(
      canonizer,
      (x) => x.payload.advertiser === t2.identity.id
    );

    // now t2 is canon, t1 and t3 is off-canon
    expect(t1.machine.mcomb().t).toBe("off-canon");
    expect(t2.machine.mcomb().t).toBe("normal");
    expect(t3.machine.mcomb().t).toBe("off-canon");

    expect(t2.machine.state().state?.[1].payload.t).toBe(Evs.L1Start);
  });
});
