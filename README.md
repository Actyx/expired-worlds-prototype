## Expired Worlds Machine

The [machine-runner model](https://github.com/Actyx/machines) explores on a swarm cooperating on a task based on a chain of distinct events.
In that model, the order of events is important.
At the same time, because of the nature of a swarm, partition happens;
consequently, different actors can perceive orders of events differently, creating perception discrepancies toward the state of the swarm task as well.
Discrepancies due to a partition can cause past actions to be invalidated in the future after the partition reconcile.

Expired Worlds Machines explore the ideas of writing compensations in the same level as the swarm protocol and facilitate actors within the swarm to both work at the task and compensating.

## Tracking States Across The Multiverse

Discrepancies creates branching event chains.
To facilitate compensations, Expired World Machines construct the perceived multiverse and simulate states across timelines to faciliate compensations.
In order to do so, Expired Worlds Machines operates on top of a special set of events which includes two types of events:

- Business Events:
  - describes the swarm task's continuity
  - has a link to its predecessor, except the primordial
  - makes up the branching chains
- Marker Events:
  - describes meta-states, states across timelines, linearly tracked
    - currently tracks only the compensations needed to be done by particular actors that denotes where to jump
  - each actor is responsible for registering their own markers

To facilitate the workflow language, Expired World Machines has a sub-VM that reacts to business events.
The multiverse record makes it possible for the Expired World Machines to spin VMs up to a freely defined point in the multiverse,
sometimes involving multiple VMs.

For example: 
A compensation is calculated by spinning two machines by piping events from different points in time.
The difference of "compensateable list" between the two points in time become the compensation needed by the actor.

// TODO: When things happen, How things are particularly calculated, The VM