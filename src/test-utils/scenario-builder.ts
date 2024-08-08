import { ActyxEvent, Tags } from "@actyx/sdk";
import {
  ActyxWFCanonAdvrt,
  CTypeProto,
  WFBusinessOrMarker,
  WFMarkerCanonAdvrt,
} from "../consts.js";
import { WFWorkflow } from "../wfcode.js";
import { Network, Node } from "../ax-mock/index.js";
import { awhile, log } from "./misc.js";
import { Machine, run } from "../index.js";
import { expect } from "@jest/globals";
import { createLinearChain } from "../event-utils.js";

/**
 * Scenario Setup Parameters
 */
export type SetupSystemParams<CType extends CTypeProto> = {
  /**
   * The initial data that should be loaded by the nodes at initialization
   */
  initialStoreData?: ActyxEvent<WFBusinessOrMarker<CType>>[];
  tags: Tags<WFBusinessOrMarker<CType>>;
};

type Identity<CType extends CTypeProto> = { id: string; role: CType["role"] };
type Agent<CType extends CTypeProto> = {
  identity: Identity<CType>;
  node: Node.Type<WFBusinessOrMarker<CType>>;
  machine: Machine<CType>;
};

export const setup = <CType extends CTypeProto>(
  code: WFWorkflow<CType>,
  setupParams: SetupSystemParams<CType>,
  params: Identity<CType>[]
) => {
  const makenetwork = Network.make<WFBusinessOrMarker<CType>>;
  const makenode = Node.make<WFBusinessOrMarker<CType>>;

  const network = makenetwork();

  const identityNodePairs = params.map((identity) => {
    // initialize nodes and load all initial data
    const node = makenode({ id: identity.id });
    node.logger.sub(log);
    if (setupParams?.initialStoreData) {
      node.store.load(setupParams.initialStoreData);
    }
    return { identity, node };
  });

  // join nodes
  identityNodePairs.forEach((x) => network.join(x.node));

  const agents = identityNodePairs.map(({ identity, node }) => {
    // run machine
    let machineInstance = run(
      { self: identity, tags: setupParams.tags },
      node,
      code
    );

    const restartMachine = async () => {
      machineInstance.kill();
      machineInstance = run(
        { self: identity, tags: setupParams.tags },
        node,
        code
      );
      await awhile();
    };

    const machineProxy = new Proxy<typeof machineInstance>(
      {} as any as typeof machineInstance,
      {
        get: (_, ...args) => Reflect.get(machineInstance, ...args),
        set: (_, ...args) => Reflect.set(machineInstance, ...args),
      }
    );

    return { identity, node, machine: machineProxy, restartMachine };
  });

  const findAgent = (fn: (c: Agent<CType>) => boolean): Agent<CType> => {
    const agent = agents.find(fn);
    if (!agent) throw new Error("findAgent error");
    return agent;
  };

  const findCommand = (agent: Agent<CType>, name: string) => {
    const found = agent.machine
      .commands()
      .find(({ info }) => info.name === name);
    if (!found)
      throw new Error(`command ${name} not found in ${agent.identity.id}`);
    return found;
  };

  const findAndRunCommand = async (
    agent: Agent<CType>,
    name: string,
    payload?: Record<string, unknown>
  ) => {
    await awhile();
    const command = findCommand(agent, name);
    command.publish(payload);
    await awhile();
  };

  const findAndRunCanonization = async (
    agent: Agent<CType>,
    findAd: (ad: ActyxWFCanonAdvrt<CType>) => boolean
  ) => {
    await awhile();
    const canonization = agent.machine
      .canonizations()
      .find((x) => findAd(x.ad));
    if (!canonization) {
      throw new Error(`canonization not found in ${agent.identity.id}`);
    }
    canonization?.publish();
    await awhile();
  };

  return {
    agents,
    findAgent,
    findCommand,
    findAndRunCommand,
    findAndRunCanonization,
    network,
  };
};

export const expectAllToHaveSameState = <CType extends CTypeProto>(
  agents: Agent<CType>[]
) => {
  const first = agents.at(0);
  if (!first) return;
  const rest = agents.slice(1);
  const firstState = first.machine.wfmachine().state();
  rest.forEach((rest) => {
    const restState = rest.machine.wfmachine().state();
    expect(firstState).toEqual(restState);
  });
};

export const expectAllToHaveSameHistory = <CType extends CTypeProto>(
  agents: Agent<CType>[]
) => {
  const first = agents.at(0);
  if (!first) return;
  const rest = agents.slice(1);
  const firstHistory = historyOf(first);
  rest.forEach((rest) => {
    const restState = historyOf(rest);
    expect(firstHistory).toEqual(restState);
  });
};

export const historyOf = <CType extends CTypeProto>(agent: Agent<CType>) => {
  const multiverse = agent.machine.multiverseTree();
  const lastState = agent.machine.wfmachine().state().state?.[1];
  if (!lastState) return [];
  return createLinearChain(multiverse, lastState);
};
