import asyncio
from collections import deque
from .models import Priority, SubQueue, Job

from .policies import (
    PriorityPolicy,
    SubQueuePolicy,
    RandomPriorityPolicy,
    AgingWeightedRandomSubQueuePolicy
)


class PriorityFairQueue:
    """Schedule jobs by priority and distribute service fairly among subqueues.

    The aging-weighted-random policy increases a subqueue's selection weight
    with each round it is not selected. This provides probabilistic fairness:
    longer-waiting active subqueues become increasingly likely to be served.
    """

    def __init__(
        self,
        priority_policy: PriorityPolicy | None = None,
        subqueue_policy: SubQueuePolicy | None = None,
    ) -> None:
        self._priority_policy = priority_policy or RandomPriorityPolicy()
        self._subqueue_policy = subqueue_policy or AgingWeightedRandomSubQueuePolicy()
        self._queues: dict[Priority, dict[str, deque[Job]]] = {}
        self._rotation: dict[Priority, list[SubQueue]] = {}
        self._condition = asyncio.Condition()

    def empty(self) -> bool:
        return not self._rotation

    async def put(self, job: Job) -> None:
        async with self._condition:
            level_queues = self._queues.setdefault(job.priority, {})
            level_queues.setdefault(job.subqueue, deque()).append(job)

            rotation = self._rotation.setdefault(job.priority, [])
            subqueue = next(
                (entry for entry in rotation if entry.name == job.subqueue),
                None,
            )
            if subqueue is None:
                rotation.append(SubQueue(name=job.subqueue, waiting_rounds=0))
            else:
                subqueue.pending_jobs += 1
            self._condition.notify()

    async def get(self) -> Job:
        async with self._condition:
            while True:
                active_priorities = [p for p, r in self._rotation.items() if r]
                if active_priorities:
                    priority = self._priority_policy.choose(active_priorities)
                    r = self._rotation[priority]

                    subqueue = self._subqueue_policy.choose(r)
                    r.remove(subqueue)
                    jobs = self._queues[priority][subqueue.name]
                    job = jobs.popleft()
                    subqueue.pending_jobs -= 1
                    if jobs:
                        r.append(subqueue)
                    else:
                        del self._queues[priority][subqueue.name]
                        if not r:
                            del self._queues[priority]
                            del self._rotation[priority]
                    # print(f"Processing job: {job.name}, priority: {job.priority}, subqueue: {job.subqueue}")
                    return job

                await self._condition.wait()
