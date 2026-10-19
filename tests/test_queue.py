import asyncio
import math
import unittest

from priomesh import Job, Priority, PriorityFairQueue, QueueShutDown
from priomesh.policies import FifoSubQueuePolicy
from priomesh.policies import AbsolutePriorityPolicy


class PriorityFairQueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_tracks_pending_jobs(self) -> None:
        queue = PriorityFairQueue()

        self.assertTrue(queue.empty())

        await queue.put(Job("job-1", Priority.NORMAL, "tenant-a"))
        await queue.put(Job("job-2", Priority.NORMAL, "tenant-a"))
        self.assertFalse(queue.empty())

        await queue.get()
        self.assertFalse(queue.empty())

        await queue.get()
        self.assertTrue(queue.empty())

    async def test_subqueue_fifo(self) -> None:
        queue = PriorityFairQueue()

        await queue.put(Job("job-1", Priority.NORMAL, "tenant-a"))
        await queue.put(Job("job-2", Priority.NORMAL, "tenant-a"))
        await queue.put(Job("job-3", Priority.NORMAL, "tenant-b"))
        await queue.put(Job("job-4", Priority.NORMAL, "tenant-c"))
        await queue.put(Job("job-5", Priority.NORMAL, "tenant-a"))
        await queue.put(Job("job-6", Priority.NORMAL, "tenant-c"))
        await queue.put(Job("job-7", Priority.NORMAL, "tenant-a"))

        jobs = []
        for _ in range(7):
            job = await queue.get()
            if job.subqueue == "tenant-a":
                jobs.append(job)
        self.assertEqual(len(jobs), 4)

        self.assertEqual(jobs[0].name, "job-1")
        self.assertEqual(jobs[1].name, "job-2")
        self.assertEqual(jobs[2].name, "job-5")
        self.assertEqual(jobs[3].name, "job-7")

    async def test_subqueue_rotation(self) -> None:
        policy = FifoSubQueuePolicy()
        queue = PriorityFairQueue(subqueue_policy=policy)

        await queue.put(Job("job-1", Priority.NORMAL, "tenant-a"))
        await queue.put(Job("job-2", Priority.NORMAL, "tenant-a"))
        await queue.put(Job("job-3", Priority.NORMAL, "tenant-b"))
        await queue.put(Job("job-4", Priority.NORMAL, "tenant-c"))
        await queue.put(Job("job-5", Priority.NORMAL, "tenant-a"))
        await queue.put(Job("job-6", Priority.NORMAL, "tenant-c"))
        await queue.put(Job("job-7", Priority.NORMAL, "tenant-a"))

        self.assertEqual((await queue.get()).name, "job-1")
        self.assertEqual((await queue.get()).name, "job-3")
        self.assertEqual((await queue.get()).name, "job-4")
        self.assertEqual((await queue.get()).name, "job-2")
        self.assertEqual((await queue.get()).name, "job-6")
        self.assertEqual((await queue.get()).name, "job-5")
        self.assertEqual((await queue.get()).name, "job-7")


    async def test_absolute_priority_policy(self) -> None:
        policy = AbsolutePriorityPolicy()
        queue = PriorityFairQueue(priority_policy=policy)

        await queue.put(Job("job-1", Priority.CRITICAL, "A"))
        await queue.put(Job("job-2", Priority.NORMAL, "A"))
        await queue.put(Job("job-3", Priority.NORMAL, "A"))
        await queue.put(Job("job-4", Priority.HIGH, "A"))
        await queue.put(Job("job-5", Priority.LOW, "A"))
        await queue.put(Job("job-6", Priority.HIGH, "A"))

        self.assertEqual((await queue.get()).name, "job-1")
        self.assertEqual((await queue.get()).name, "job-4")
        self.assertEqual((await queue.get()).name, "job-6")
        self.assertEqual((await queue.get()).name, "job-2")
        self.assertEqual((await queue.get()).name, "job-3")
        self.assertEqual((await queue.get()).name, "job-5")

    async def test_empty_wait_and_notify(self) -> None:
        queue = PriorityFairQueue()
        job = Job("job-1", Priority.NORMAL)

        # 创建一个正在等待取任务的协程。
        task_get = asyncio.create_task(queue.get())

        # 让出执行权，让 task_get 有机会运行并进入等待
        await asyncio.sleep(0.1)
        self.assertFalse(task_get.done())

        # 放入任务后，之前等待的 task_get 应该被唤醒并拿到它
        await queue.put(job)
        result = await asyncio.wait_for(task_get, timeout=1)
        self.assertEqual(result, job)

    async def test_concurrent_put_get_preserves_all_jobs(self) -> None:
        queue = PriorityFairQueue()

        producer_count = 4
        jobs_per_producer = 25
        consumer_count = 5
        jobs_per_consumer = 20

        batches = [
            [
                Job(
                    name=f"producer-{producer}-job-{job_number}",
                    priority=Priority.NORMAL,
                    subqueue=f"tenant-{producer}"
                )
                for job_number in range(jobs_per_producer)
            ]
            for producer in range(producer_count)
        ]

        async def produce(jobs: list[Job]) -> None:
            for job in jobs:
                await queue.put(job)

        async def consume(count: int) -> list[Job]:
            # 未拿到足够的会卡住
            return [
                await queue.get()
                for _ in range(count)
            ]

        consumer_tasks = [
            asyncio.create_task(consume(jobs_per_consumer))
            for _ in range(consumer_count)
        ]

        await asyncio.wait_for(
            asyncio.gather(
                *(produce(batch) for batch in batches),
                *consumer_tasks
            ),
            timeout=5
        )

        received_names = [
            job.name
            for task in consumer_tasks
            for job in task.result()
        ]

        expected_names = [
            job.name
            for batch in batches
            for job in batch
        ]
        self.assertEqual(len(received_names), producer_count * jobs_per_producer)
        self.assertCountEqual(received_names, expected_names)
        self.assertTrue(queue.empty())

    async def test_get_from_shutdown_queue(self) -> None:
        queue = PriorityFairQueue()
        await queue.put(Job("job-1", Priority.NORMAL))
        await queue.put(Job("job-2", Priority.NORMAL))
        await queue.put(Job("job-3", Priority.NORMAL))

        await queue.shutdown()

        self.assertEqual((await queue.get()).name, "job-1")
        self.assertEqual((await queue.get()).name, "job-2")
        self.assertEqual((await queue.get()).name, "job-3")
        with self.assertRaises(QueueShutDown):
            await queue.get()

    async def test_put_to_shutdown_queue(self) -> None:
        queue = PriorityFairQueue()
        await queue.shutdown()

        with self.assertRaises(QueueShutDown):
            await queue.put(Job("job-1", Priority.NORMAL))

    async def test_wait_waiter(self) -> None:
        queue = PriorityFairQueue()

        task = asyncio.create_task(queue.get())
        await asyncio.sleep(0.01)
        self.assertFalse(task.done())

        await queue.shutdown()
        with self.assertRaises(QueueShutDown):
            await asyncio.wait_for(task, timeout=1)

    async def test_wait_mul_waiters(self) -> None:
        queue = PriorityFairQueue()

        task1 = asyncio.create_task(queue.get())
        task2 = asyncio.create_task(queue.get())
        task3 = asyncio.create_task(queue.get())
        await asyncio.sleep(0.01)
        self.assertFalse(task1.done())
        self.assertFalse(task2.done())
        self.assertFalse(task3.done())

        await queue.shutdown()
        for t in (task1, task2, task3):
            with self.assertRaises(QueueShutDown):
                await asyncio.wait_for(t, timeout=1)

    async def test_maxsize_neg(self) -> None:
        with self.assertRaises(ValueError):
            PriorityFairQueue(maxsize=-1)

    async def test_maxsize_zero(self) -> None:
        queue = PriorityFairQueue(maxsize=0)
        for i in range(10):
            await queue.put(Job(f"job-{i}", Priority.NORMAL))
        self.assertEqual(queue.qsize(), 10)
        self.assertFalse(queue.full())
        while not queue.empty():
            await queue.get()
        self.assertEqual(queue.qsize(), 0)

    async def test_qsize_and_full_track_jobs(self) -> None:
        queue = PriorityFairQueue(maxsize=2)

        self.assertEqual(queue.qsize(), 0)
        self.assertFalse(queue.full())

        await queue.put(Job("job-1", Priority.NORMAL, "tenant-a"))
        self.assertEqual(queue.qsize(), 1)
        self.assertFalse(queue.full())

        await queue.put(Job("job-2", Priority.HIGH, "tenant-b"))
        self.assertEqual(queue.qsize(), 2)
        self.assertTrue(queue.full())

        await queue.get()
        self.assertEqual(queue.qsize(), 1)
        self.assertFalse(queue.full())

        await queue.get()
        self.assertEqual(queue.qsize(), 0)
        self.assertFalse(queue.full())

    async def test_full_is_false_for_unbounded_queue(self) -> None:
        queue = PriorityFairQueue()
        await queue.put(Job("job-1", Priority.NORMAL))

        self.assertEqual(queue.qsize(), 1)
        self.assertFalse(queue.full())

    async def test_maxsize_1(self) -> None:
        queue = PriorityFairQueue(maxsize=1)
        await queue.put(Job("job-1", Priority.NORMAL))

        t = asyncio.create_task(
            queue.put(Job("job-2", Priority.NORMAL))
        )
        await asyncio.sleep(0)
        self.assertFalse(t.done())
        await queue.get()

        await asyncio.wait_for(t, timeout=1)
        await queue.get()
        self.assertTrue(t.done())
        self.assertTrue(queue.empty())

    async def test_shutdown_wakes_put_waiting_on_full_queue(self) -> None:
        queue = PriorityFairQueue(maxsize=1)
        await queue.put(Job("job-1", Priority.NORMAL))

        put_task = asyncio.create_task(
            queue.put(Job("job-2", Priority.NORMAL))
        )
        await asyncio.sleep(0)
        self.assertFalse(put_task.done())

        await queue.shutdown()

        with self.assertRaises(QueueShutDown):
            await asyncio.wait_for(put_task, timeout=1)

        self.assertEqual((await queue.get()).name, "job-1")
        with self.assertRaises(QueueShutDown):
            await queue.get()

    async def test_get_times_out_when_queue_is_empty(self) -> None:
        queue = PriorityFairQueue()

        with self.assertRaises(asyncio.TimeoutError):
            await queue.get(timeout=0.01)

        self.assertTrue(queue.empty())
        self.assertEqual(queue.qsize(), 0)

        job = Job("job-1", Priority.NORMAL)
        await queue.put(job)
        self.assertEqual(await queue.get(), job)

    async def test_put_times_out_when_queue_is_full(self) -> None:
        queue = PriorityFairQueue(maxsize=1)
        first_job = Job("job-1", Priority.NORMAL)
        await queue.put(first_job)

        with self.assertRaises(asyncio.TimeoutError):
            await queue.put(Job("job-2", Priority.NORMAL), timeout=0.01)

        self.assertEqual(queue.qsize(), 1)
        self.assertTrue(queue.full())
        self.assertEqual(await queue.get(), first_job)

    async def test_cancelled_get_does_not_remove_jobs(self) -> None:
        queue = PriorityFairQueue()
        get_task = asyncio.create_task(queue.get())
        await asyncio.sleep(0)
        self.assertFalse(get_task.done())

        get_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await get_task

        job = Job("job-1", Priority.NORMAL)
        await queue.put(job)
        self.assertEqual(queue.qsize(), 1)
        self.assertEqual(await queue.get(), job)

    async def test_cancelled_put_does_not_enqueue_job(self) -> None:
        queue = PriorityFairQueue(maxsize=1)
        first_job = Job("job-1", Priority.NORMAL)
        await queue.put(first_job)

        put_task = asyncio.create_task(
            queue.put(Job("job-2", Priority.NORMAL))
        )
        await asyncio.sleep(0)
        self.assertFalse(put_task.done())

        put_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await put_task

        self.assertEqual(queue.qsize(), 1)
        self.assertTrue(queue.full())
        self.assertEqual(await queue.get(), first_job)

    async def test_put_rejects_invalid_timeouts(self) -> None:
        queue = PriorityFairQueue()
        job = Job("job-1", Priority.NORMAL)

        for timeout in (0, -1, math.inf, -math.inf, math.nan):
            with self.subTest(timeout=timeout):
                with self.assertRaises(ValueError):
                    await queue.put(job, timeout=timeout)

        self.assertTrue(queue.empty())
        self.assertEqual(queue.qsize(), 0)

    async def test_get_rejects_invalid_timeouts_without_consuming_jobs(self) -> None:
        queue = PriorityFairQueue()
        job = Job("job-1", Priority.NORMAL)
        await queue.put(job)

        for timeout in (0, -1, math.inf, -math.inf, math.nan):
            with self.subTest(timeout=timeout):
                with self.assertRaises(ValueError):
                    await queue.get(timeout=timeout)

        self.assertEqual(queue.qsize(), 1)
        self.assertEqual(await queue.get(), job)

if __name__ == "__main__":
    """
    python -m pip install .
    python -m unittest discover -s tests -v

    or
    PYTHONPATH=src python -m unittest discover -s tests -v
    """
    unittest.main()
