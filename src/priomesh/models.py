# Job, Priority, SubQueue
from dataclasses import dataclass
from enum import IntEnum


class Priority(IntEnum):
    CRITICAL = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4


@dataclass(frozen=True)
class Job:
    name: str
    priority: Priority
    subqueue: str


@dataclass(frozen=False)
class SubQueue:
    name: str
    waiting_rounds: int