import { ActyxWFBusiness, CTypeProto, sortByEventKey } from "./consts.js";
import { MultiverseTree } from "./reality.js";

/**
 * Linear chain = chain where parallels are ignored.
 */
export const createLinearChain = <CType extends CTypeProto>(
  multiverse: MultiverseTree.Type<CType>,
  point: ActyxWFBusiness<CType>
) => {
  const chain = [point];
  while (true) {
    const first = chain[0];
    // Note(Alan): this covers parallel's special case where the concluding
    // event has many predecessors. The earliest predecessor must be the
    // triggering event.
    const predecessor = sortByEventKey(
      multiverse
        .getPredecessors(first.meta.eventId)
        .filter(
          (x): x is ActyxWFBusiness<CType> =>
            x !== MultiverseTree.UnregisteredEvent
        )
    ).at(0);
    if (!predecessor) break;
    chain.unshift(predecessor);
  }
  return chain;
};
