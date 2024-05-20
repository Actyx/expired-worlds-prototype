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

