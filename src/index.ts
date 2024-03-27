import {
  Actyx,
  ActyxEvent,
  CancelSubscription,
  EventsOrTimetravel,
  MonotonicSubscription,
  MsgType,
  OnCompleteOrErr,
} from "@actyx/sdk";

import * as Reality from "./reality.js";

export type Params<E> = {
  actyx: Parameters<(typeof Actyx)["of"]>;
  query: MonotonicSubscription<E>;
};
type OnEventsOrTimetravel<E> = (data: EventsOrTimetravel<E>) => Promise<void>;

export const run = async <E = unknown>(params: Params<E>) => {
  const actyx = await Actyx.of(...params.actyx);
  let cancel: CancelSubscription | null = null;

  const witness = Reality.witness<ActyxEvent<E>>({
    id: (e) => e.meta.eventId,
  });

  /**
   * Connect to Actyx with predefined params and keep connection until closed.
   */
  const connect = () => {
    if (cancel === null) {
      const onEventsOrTimeTravel: OnEventsOrTimetravel<E> = async (e) => {
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

      cancel = actyx.subscribeMonotonic<E>(
        params.query,
        onEventsOrTimeTravel,
        onCompleteOrErr
      );
    }
  };

  connect();

  return {
    cancel: () => cancel?.(),
    canonReality: witness.canonReality,
  };
};
