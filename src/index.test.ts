// import { describe, expect, it } from "@jest/globals";
// import { Reality } from "./index.js";
// import { v4 as uuidv4 } from "uuid";

// type EventType = { id: string };
// const event1: EventType = { id: uuidv4() };
// const event2: EventType = { id: uuidv4() };
// const event3: EventType = { id: uuidv4() };
// const event4: EventType = { id: uuidv4() };
// const event5: EventType = { id: uuidv4() };

// const identify = (e: EventType): string => e.id;

// describe("witness", () => {
//   it("should track if reality is canon", async () => {
//     const witness = Reality.witness<EventType>({ id: identify });

//     witness.see(event2);

//     const reality = await witness.canonReality();

//     expect(reality.isCanon()).toBe(true);
//     expect(reality.isExpired()).toBe(false);

//     witness.retrospect();

//     expect(reality.isCanon()).toBe(false);
//     expect(reality.isExpired()).toBe(true);
//   });

//   it("should work", async () => {
//     const witness = Reality.witness<EventType>({ id: identify });

//     witness.see(event2);
//     witness.see(event3);
//     witness.see(event5);

//     const reality1 = await witness.canonReality();

//     witness.retrospect();

//     witness.see(event1);
//     witness.see(event2);
//     witness.see(event3);
//     witness.see(event4);
//     witness.see(event5);

//     const amendments = reality1.amendments();
//     expect(amendments).not.toBe(Reality.AmendmentsNotReady);
//     if (amendments === Reality.AmendmentsNotReady) throw new Error("fail");

//     const first = amendments.at(0);
//     expect(first).toBeTruthy();
//     if (!first) throw new Error("fail");

//     expect(first.past.history()).toEqual([event2, event3, event5]);
//     expect(first.future.history()).toEqual([
//       event1,
//       event2,
//       event3,
//       event4,
//       event5,
//     ]);
//     expect(first.divergentPoint).toEqual(event2);

//     const reality2 = await witness.canonReality();
//     expect(reality1.history() === first.past.history());
//     expect(reality2.history() === first.future.history());
//   });

//   it("should track latest data", async () => {
//     const witness = Reality.witness<EventType>({ id: identify });

//     witness.see(event1);
//     const reality = await witness.canonReality();

//     expect(reality.latest()).toBe(event1);
//     witness.see(event2);
//     expect(reality.latest()).toBe(event2);
//     witness.see(event3);
//     expect(reality.latest()).toBe(event3);
//     witness.see(event4);
//     expect(reality.latest()).toBe(event4);
//     witness.see(event5);
//     expect(reality.latest()).toBe(event5);
//   });

//   it("should track previous snapshots", async () => {
//     const witness = Reality.witness<EventType>({ id: identify });

//     witness.see(event1);
//     witness.see(event2);
//     witness.see(event3);
//     witness.see(event4);
//     witness.see(event5);

//     const reality = await witness.canonReality();
//     const snap4 = reality.previous();
//     expect(snap4?.latest()).toBe(event4);
//     const snap3 = snap4?.previous();
//     expect(snap3?.latest()).toBe(event3);
//     const snap2 = snap3?.previous();
//     expect(snap2?.latest()).toBe(event2);
//     const snap1 = snap2?.previous();
//     expect(snap1?.latest()).toBe(event1);
//     const snapnull = snap1?.previous();
//     expect(snapnull).toBe(null);
//   });

//   it("should track root", async () => {
//     const witness = Reality.witness<EventType>({ id: identify });

//     witness.see(event1);
//     witness.see(event2);
//     witness.see(event3);
//     witness.see(event4);
//     witness.see(event5);

//     const reality = await witness.canonReality();
//     expect(reality.first().latest()).toBe(event1);
//   });

//   it("should be able to get reality from snapshot", async () => {
//     const witness = Reality.witness<EventType>({ id: identify });

//     witness.see(event1);
//     witness.see(event2);
//     witness.see(event3);
//     witness.see(event4);
//     witness.see(event5);

//     const reality = await witness.canonReality();
//     expect(reality.first().real()).toBe(reality);
//   });
// });
