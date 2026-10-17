import random
from collections.abc import Sequence
from typing import Protocol
from ..models import SubQueue


class SubQueuePolicy(Protocol):
    def choose(self, subqueues: Sequence[SubQueue]) -> SubQueue:
        """Choose one currently active subqueue."""


class AgingWeightedRandomSubQueuePolicy:
    """Favor subqueues that have waited through more scheduling rounds."""

    def choose(self, subqueues: Sequence[SubQueue]) -> SubQueue:
        selected = random.choices(
            subqueues,
            weights=[subqueue.waiting_rounds + 1 for subqueue in subqueues],
            k=1
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