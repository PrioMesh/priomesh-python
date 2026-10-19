import asyncio
import math
from collections import deque
from .models import Priority, SubQueue, Job

from .policies import (
    PriorityPolicy,
    SubQueuePolicy,
    RandomPriorityPolicy,
    AgingWeightedRandomSubQueuePolicy
)


class QueueShutDown(Exception):
    """Raised when an operation is attempted on a shut-down queue."""


def _validate_timeout(timeout: float | None) -> None:
    if timeout is not None and (not math.isfinite(timeout) or timeout <= 0):
        raise ValueError("timeout must be a finite positive number or None")


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
        maxsize: int = 0,
    ) -> None:
        self._priority_policy = priority_policy or RandomPriorityPolicy()
        self._subqueue_policy = subqueue_policy or AgingWeightedRandomSubQueuePolicy()
        self._queues: dict[Priority, dict[str, deque[Job]]] = {}
        self._rotation: dict[Priority, list[SubQueue]] = {}
        self._condition = asyncio.Condition()
        self._shutdown = False

        if maxsize < 0:
            raise ValueError("maxsize must be non-negative")
        self._maxsize = maxsize
        self._size = 0

    def empty(self) -> bool:
        return not self._rotation

    async def put(self, job: Job, timeout: float | None = None) -> None:
        _validate_timeout(timeout)
        if timeout is None:
            await self._put(job)
        else:
            await asyncio.wait_for(self._put(job), timeout=timeout)

    async def _put(self, job: Job) -> None:
        async with self._condition:
            while self._maxsize > 0 and self._size >= self._maxsize:
                if self._shutdown:
                    raise QueueShutDown("cannot put into a shut-down queue")
                await self._condition.wait()

            if self._shutdown:
                raise QueueShutDown("cannot put into a shut-down queue")
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
            self._size += 1
            self._condition.notify()

    async def get(self, timeout: float | None = None) -> Job:
        _validate_timeout(timeout)
        if timeout is None:
            return await self._get()
        return await asyncio.wait_for(self._get(), timeout=timeout)

    async def _get(self) -> Job:
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
                    self._size -= 1
                    self._condition.notify_all()
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

                if self._shutdown:
                    raise QueueShutDown("queue is shut down and empty")
                await self._condition.wait()

    async def shutdown(self) -> None:
        async with self._condition:
            self._shutdown = True
            self._condition.notify_all()  # 唤醒所有等待的协程，促使其检查关闭态并退出

    def qsize(self) -> int:
        return self._size

    def full(self) -> bool:
        return self._maxsize > 0 and self._size >= self._maxsize