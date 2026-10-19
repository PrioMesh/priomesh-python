import math
import random
from collections.abc import Sequence
from typing import Protocol
from ..models import SubQueue


class SubQueuePolicy(Protocol):
    def choose(self, subqueues: Sequence[SubQueue]) -> SubQueue:
        """Choose one currently active subqueue."""


class AgingWeightedRandomSubQueuePolicy:
    """Favor larger backlogs while increasing chances for waiting subqueues.

    Each active subqueue's weight is sqrt(pending jobs) * (waiting rounds + 1).
    The square root gives deeper queues more service opportunities without
    making a small queue's chance negligible. Aging raises the weight of a
    subqueue each time it is not selected and resets it when it is selected.
    Selection remains probabilistic; this policy does not impose a wait limit.
    """

    def choose(self, subqueues: Sequence[SubQueue]) -> SubQueue:
        weights = [
            math.sqrt(subqueue.pending_jobs) * (subqueue.waiting_rounds + 1)
            for subqueue in subqueues
        ]
        selected = random.choices(
            subqueues,
            weights=weights,
            k=1,
        )[0]
        for subqueue in subqueues:
            if subqueue is selected:
                subqueue.waiting_rounds = 0
            else:
                subqueue.waiting_rounds += 1
        return selected


class RandomSubQueuePolicy:
    """Choose an active subqueue uniformly at random."""

    def choose(self, subqueues: Sequence[SubQueue]) -> SubQueue:
        return random.choice(subqueues)


class FifoSubQueuePolicy:
    """Choose the subqueue at the front of the rotation."""

    def choose(self, subqueues: Sequence[SubQueue]) -> SubQueue:
        return subqueues[0]