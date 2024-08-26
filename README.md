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

## [2024-05-10] Problem with Linear VM and Parallel

I just realized this.
Predecessor tagging is created at publish time and is not regulated by Actyx's lamport timestamp. As a consequence, any code branches (e.g PARALLEL, TIMEOUT) can create a branch in the timeline too. For example:
the code below with 1 M and 3 Ts

```
request @ M
PARALLEL {
  bid @ T
}
```

Can produce varying predecessor arrows

```
     Request @ M1        Request @ M1
         │                   │
    ┌────┴─────┐             │
    ▼          ▼             ▼
Bid @ T1    Bid @ T3     Bid @ T1
    │                        │
    ▼                        ▼
Bid @ T2                 Bid @ T2
                             │
                             ▼
                         Bid @ T3
```

The entirety of the machine should be able to process not a linear log but a tree in case of parallels and regulate the predecessor arrows so that in the case of parallel, this should be constructed instead.

```
         Request @ M1
               │
    ┌──────────┼─────────┐
    │          │         │
    ▼          ▼         ▼
 Bid @ T1  Bid @ T2  Bid @ T3
```

## Event Marking the Closing of Parallel

A machine moves on from a parallel block when an event after the parallel is detected.
Let's call this "closing"; while this does not really close the parallel from having more instance being added to it, due to possibilities of future time travel, "closing" will just move the machine away and ignore the parallel from the time being.

Parallel consumes several branches in the multiverse.
Intuitively the closing event must be preceeded by one of the branch in the multiverse.
However that cannot happen because a machine must choose one of the branch and there is no mechanism for that (at least for non participant of a particular branch).

Instead the closing event must follow the predecessor just like the other branches.

A parallel is interpreted as a rich state triggered by a particular event predecessor that allows the machine to peers into multiple realities it produces, but the event itself is still a part of the main branch which the multiple realities are perceived as to collapse into.

## [2024-08-01] Divergence at the beginning?

Every workflow instance needs an ID. This ID will also become the ID of the subscription as well.

Assigning the ID of the first event's ID as the instance's ID makes things easy: e.g. we can generate a query code based on the workflow definition alone.

The consequence is: there can be no divergence in the beginning, despite the multiverse tree's support for multiple roots.

## Some Idea: Canon Override

Since we now have the distinction between meta/marker events and business events, this idea of canon override might work.
The core of this idea is that there might be a reason a swarm want to decide that a particular timeline might be the timeline it wants to follow despite of the native ordering rule of Actyx, the EventKey ordering.

For example, a partition happens, creating two groups, A and B. B has progressed far and done a lot of compensateable tasks. But because Actyx's event ordering mechanism prioritize an event with a smaller stream key when the timestamp is the same, A might be picked over B, which causes B's initiated timeline to be expired; and as the consequence, B must run a lot of compensations.

Canon override allows the swarm to pre-assign a "canon-dictator" before possible
points of divergence (e.g. PARALLEL, TIMEOUT, RETRY-FAIL) and let this
"canon-dictator" dictates a canon timeline when the business needs it.

![mechanics of canon-override](ideas-assets\canon-override.png)

## [2024-08-26] Important bits

In hindsight the important bits of this computation model are:

- The workflow code.
- The multiverse tree and meta log.
- Multiple instantiations of WFMachines.

A WFMachine takes a workflow code and form a state machine. This state machine can determine the `nexts`, the set of expected events for one particular state among all possible states in the machine-code combination. Each next-event is a path where the machine can go. Partition in the system can cause machines to go in two different direction, this is what makes the branches in the tree.

The meta log is a classic Actyx's one-log-per-subscription append log. Events in this special log is used to determine how the swarm should interpret the multiverse tree. By default, the canon branch in the multiverse tree is chosen by comparing the events' EventKey (especially the Stream ID part, since the lamport timestamp is I think always the same). The meta log allows an actor to "canonize" a branch in the swarm, overriding the EventKey ordering mechanism.

The meta log is also useful for notating owned responsibility. In the case of compensations, the actors publish an event as a note to state that "I should compensate for the decanonization from this particular branch", but an actor doesn't publish an event that states "my neighbor should compensate for ...". This works because with Actyx one can know if an event is published by yourself or another.

One particular aspect in compensations that I just realized not being implemented is that an actor can calculate whether members in the swarm can resolve a compensation. For example, three actors are supposed to be involved in a compensation scheme, but one of them, due to an accident such as power outage, does not publish a `compensation:needed` event. The other party can see the list of `compensation:needed` events and determine if the currently present members in the swarm can solve the scheme or not. It will not, however, be able to determine if a scheme is finishable or unfinishable in the infinitely distant future.
