import asyncio
import unittest
from collections.abc import Sequence
from unittest.mock import patch

from priomesh import Job, Priority, PriorityFairQueue, SubQueue
from priomesh.policies import FifoSubQueuePolicy, AgingWeightedRandomSubQueuePolicy, RandomSubQueuePolicy
from priomesh.policies import AbsolutePriorityPolicy, RandomPriorityPolicy


class PriorityFairQueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_absolute_priority_policy(self) -> None:
        policy = AbsolutePriorityPolicy()
        queue = PriorityFairQueue(priority_policy=policy)

        await queue.put(Job("job-1", Priority.NORMAL, "tenant-a"))
        await queue.put(Job("job-2", Priority.NORMAL, "tenant-a"))
        await queue.put(Job("job-3", Priority.HIGH, "tenant-a"))
        await queue.put(Job("job-4", Priority.LOW, "tenant-a"))

        self.assertEqual((await queue.get()).name, "job-3")
        self.assertEqual((await queue.get()).name, "job-1")
        self.assertEqual((await queue.get()).name, "job-2")
        self.assertEqual((await queue.get()).name, "job-4")

        self.assertTrue(queue.empty())

    async def test_random_priority_policy(self) -> None:
        policy = RandomPriorityPolicy()
        queue = PriorityFairQueue(priority_policy=policy)
        normal_jobs = [
            Job(f"job-normal-{i}", Priority.NORMAL)
            for i in range(1000)
        ]
        high_jobs = [
            Job(f"job-high-{i}", Priority.HIGH)
            for i in range(1000)
        ]
        for job in [*normal_jobs, *high_jobs]:
            await queue.put(job)

        receive_normal_count = receive_high_count = 0
        for _ in range(1000):
            job = await queue.get()
            if job.name.startswith("job-normal-"):
                receive_normal_count += 1
            else:
                receive_high_count += 1
        self.assertTrue(receive_high_count > 500)
        self.assertTrue(receive_normal_count < 500)

    async def test_subqueue_fifo_policy(self) -> None:
        policy = FifoSubQueuePolicy()
        queue = PriorityFairQueue(subqueue_policy=policy)

        await queue.put(Job("job-1", Priority.NORMAL, "tenant-a"))
        await queue.put(Job("job-2", Priority.NORMAL, "tenant-a"))
        await queue.put(Job("job-3", Priority.NORMAL, "tenant-b"))
        await queue.put(Job("job-4", Priority.NORMAL, "tenant-a"))
        await queue.put(Job("job-5", Priority.NORMAL, "tenant-c"))
        await queue.put(Job("job-6", Priority.NORMAL, "tenant-b"))

        self.assertEqual((await queue.get()).name, "job-1")
        self.assertEqual((await queue.get()).name, "job-3")
        self.assertEqual((await queue.get()).name, "job-5")
        self.assertEqual((await queue.get()).name, "job-2")
        self.assertEqual((await queue.get()).name, "job-6")
        self.assertEqual((await queue.get()).name, "job-4")

    async def test_custom_for_aging_policy(self) -> None:
        policy = AgingWeightedRandomSubQueuePolicy()
        queue = PriorityFairQueue(
            priority_policy=AbsolutePriorityPolicy(),
            subqueue_policy=policy,
        )
        await queue.put(Job("job-1", Priority.NORMAL, "tenant-a"))
        await queue.put(Job("job-2", Priority.NORMAL, "tenant-b"))
        await queue.put(Job("job-3", Priority.NORMAL, "tenant-c"))
        await queue.put(Job("job-4", Priority.NORMAL, "tenant-c"))
        await queue.put(Job("job-5", Priority.NORMAL, "tenant-c"))
        await queue.put(Job("job-6", Priority.NORMAL, "tenant-c"))
        await queue.put(Job("job-7", Priority.NORMAL, "tenant-c"))
        await queue.put(Job("job-8", Priority.NORMAL, "tenant-c"))
        await queue.put(Job("job-9", Priority.NORMAL, "tenant-c"))

        # c 的概率很大
        # self.assertEqual((await queue.get()).name, "job-3")
        while queue.empty():
            await queue.get()

    def test_subqueue_aging_policy_weights_backlog_and_wait(self) -> None:
        policy = AgingWeightedRandomSubQueuePolicy()
        bulk = SubQueue("bulk", waiting_rounds=0, pending_jobs=100)
        small = SubQueue("small", waiting_rounds=0, pending_jobs=1)

        with patch(
            "priomesh.policies.subqueue.random.choices",
            side_effect=[[small], [small]],
        ) as choices:
            self.assertIs(policy.choose([bulk, small]), small)
            self.assertEqual(bulk.waiting_rounds, 1)
            self.assertEqual(small.waiting_rounds, 0)

            self.assertIs(policy.choose([bulk, small]), small)

        self.assertEqual(choices.call_args_list[0].kwargs["weights"], [10.0, 1.0])
        self.assertEqual(choices.call_args_list[1].kwargs["weights"], [20.0, 1.0])

    async def test_queue_updates_backlog_for_aging_policy(self) -> None:
        policy = AgingWeightedRandomSubQueuePolicy()
        queue = PriorityFairQueue(
            priority_policy=AbsolutePriorityPolicy(),
            subqueue_policy=policy,
        )
        jobs = [
            *(Job(f"bulk-{i}", Priority.NORMAL, "bulk") for i in range(4)),
            *(Job(f"small-{i}", Priority.NORMAL, "small") for i in range(2)),
        ]
        for job in jobs:
            await queue.put(job)

        def choose_small(candidates: Sequence[SubQueue], **_: object) -> list[SubQueue]:
            return [next(sq for sq in candidates if sq.name == "small")]

        with patch(
            "priomesh.policies.subqueue.random.choices",
            side_effect=choose_small,
        ) as choices:
            await queue.get()
            await queue.get()

        self.assertEqual(choices.call_args_list[0].kwargs["weights"], [2.0, 2**0.5])
        self.assertEqual(choices.call_args_list[1].kwargs["weights"], [4.0, 1.0])

    def test_subqueue_random_policy(self) -> None:
        policy = RandomSubQueuePolicy()
        subqueues = [
            SubQueue("tenant-a", waiting_rounds=2),
            SubQueue("tenant-b", waiting_rounds=5),
        ]

        with patch(
            "priomesh.policies.subqueue.random.choice",
            return_value=subqueues[1],
        ) as choice:
            result = policy.choose(subqueues)

        self.assertIs(result, subqueues[1])
        choice.assert_called_once_with(subqueues)
        self.assertEqual([sq.waiting_rounds for sq in subqueues], [2, 5])


if __name__ == "__main__":
    """
    python -m pip install .
    python -m unittest discover -s tests -v

    or
    PYTHONPATH=src python -m unittest discover -s tests -v
    """
    unittest.main()