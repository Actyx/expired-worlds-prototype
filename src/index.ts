import {
  Actyx,
  ActyxEvent,
  CancelSubscription,
  EventKey,
  EventsOrTimetravel,
  MsgType,
  OnCompleteOrErr,
  Tags,
} from "@actyx/sdk";

import * as Reality from "./reality.js";
import { CTypeProto, Emit, WFMachine, WFWorkflow } from "./wfmachine.js";
import { sleep } from "./utils.js";
export { Reality };

export type Params<CType extends CTypeProto> = {
  actyx: Parameters<(typeof Actyx)["of"]>;
  tags: Tags<Ev<CType["ev"]>>;
  self: CType["role"];
};
type Ev<T extends string> = {
  t: T;
  payload: Record<string, unknown>;
  predecessorId: string;
};
type OnEventsOrTimetravel<E> = (data: EventsOrTimetravel<E>) => Promise<void>;

export const run = async <CType extends CTypeProto>(
  params: Params<CType>,
  workflow: WFWorkflow<CType>
) => {
  type TheEv = Ev<CType["ev"]>;
  const actyx = await Actyx.of(...params.actyx);
  let cancel: CancelSubscription | null = null;

  const witness = Reality.witness<ActyxEvent<TheEv>>({
    id: (e) => e.meta.eventId,
  });

  /**
   * Connect to Actyx with predefined params and keep connection until closed.
   */
  const connect = () => {
    if (cancel === null) {
      const onEventsOrTimeTravel: OnEventsOrTimetravel<TheEv> = async (e) => {
        if (e.type === MsgType.timetravel) {
          witness.retrospect();
        }

        if (e.type === MsgType.events) {
          e.events.map((e) => witness.see(e));
        }
      };

      const onCompleteOrErr: OnCompleteOrErr = (err) => {
        if (err) {
          cancel = null;
          connect();
        }
      };

      cancel = actyx.subscribeMonotonic<TheEv>(
        {
          sessionId: "dummy",
          attemptStartFrom: { from: {}, latestEventKey: EventKey.zero },
          query: params.tags,
        },
        onEventsOrTimeTravel,
        onCompleteOrErr
      );
    }
  };

  let currentWfMachine = WFMachine<CType>(workflow);
  let nextCanonEventIndexToSwallow = 0;
  let witnessCanon: null | Reality.Reality<ActyxEvent<TheEv>> = null;
  let amendmentsMode: null | {
    nextSimulatedWFMachine: WFMachine<CType>;
    witnessCanon: Reality.Reality<ActyxEvent<TheEv>>;
    nextCanonEventIndexToSwallow: number;
  } = null;
  let alive = false;

  const exitPromise = (async () => {
    // TODO: change to reactive pattern
    while (alive) {
      const activeCanon: Reality.Reality<ActyxEvent<TheEv>> = (witnessCanon =
        witnessCanon || (await witness.canonReality()));

      if (!amendmentsMode) {
        // Update WFMachine
        const acquiredEvents = activeCanon
          .history()
          .slice(nextCanonEventIndexToSwallow);

        nextCanonEventIndexToSwallow += acquiredEvents.length;
        acquiredEvents.forEach((e) =>
          currentWfMachine.tick(Emit.event(e.payload.t, e.payload.payload))
        );

        // check for possible amendments
        if (activeCanon.isExpired()) {
          const directAmendments = activeCanon.directAmendments();
          if (
            directAmendments !== null &&
            directAmendments !== Reality.AmendmentsNotReady
          ) {
            const nextSimulatedMachine = WFMachine(workflow);
            const futureHistory = directAmendments.future.history();

            futureHistory.forEach((e) =>
              currentWfMachine.tick(Emit.event(e.payload.t, e.payload.payload))
            );

            // NOTE: I assume this is how compensation should work
            // TODO: deepEqual here
            if (
              currentWfMachine.state().state !==
                nextSimulatedMachine.state().state &&
              currentWfMachine
                .availableCompensations()
                .filter((x) => x.role === params.self).length > 0
            ) {
              // amendments mode is active when the state between your reality
              // and the supposed latest reality is different and there's a
              // compensation involve?
              amendmentsMode = {
                nextSimulatedWFMachine: nextSimulatedMachine,
                witnessCanon: directAmendments.future,
                nextCanonEventIndexToSwallow: futureHistory.length,
              };
            } else {
              // otherwise, just replace the machine and canon with the supposed latest one
              currentWfMachine = nextSimulatedMachine;
              witnessCanon = directAmendments.future;
              nextCanonEventIndexToSwallow = futureHistory.length;
            }
          }
        }
      } else {
        // if amendmentsMode
        // TODO: wake sleep if amendments called
        await sleep(100);
      }

      await sleep(10);
    }
  })();

  connect();

  const commands = () =>
    currentWfMachine
      .availableCommands()
      .filter((x) => x.role === params.self)
      .map((x) => ({
        info: x,
        publish: <T extends Ev<CType["ev"]>>(payload: T["payload"]) =>
          actyx.publish(params.tags.apply({ t: x.name, payload })),
      }));

  return {
    amendment: () => {
      if (!amendmentsMode) {
        return null;
      }
      return {
        compensations: currentWfMachine
          .availableCompensations()
          .filter((x) => x.role === params.self),
      };
    },
    commands,
    command: (name: CType["ev"]) =>
      commands().find((x) => x.info.name === name) || null,
    exit: async () => {
      alive = false;
      await exitPromise;
      cancel?.();
    },
  };
};
