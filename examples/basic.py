import asyncio

from priomesh import Job, Priority, PriorityFairQueue



async def main() -> None:
    queue = PriorityFairQueue()
    jobs = [
        Job(name="job1", priority=Priority.HIGH, subqueue="sub-q-1"),
        Job(name="job2", priority=Priority.HIGH, subqueue="sub-q-2"),
        Job(name="job3", priority=Priority.HIGH, subqueue="sub-q-2"),
        Job(name="job4", priority=Priority.LOW, subqueue="sub-q-3"),
        Job(name="job5", priority=Priority.LOW, subqueue="sub-q-4"),
        Job(name="job6", priority=Priority.LOW, subqueue="sub-q-4"),
    ]

    for job in jobs:
        await queue.put(job)

    for _ in jobs:
        job = await queue.get()
        print(f"{job.priority.name:8} {job.subqueue:8} {job.name}")

if __name__ == "__main__":
    asyncio.run(main())