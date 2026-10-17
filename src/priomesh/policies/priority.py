import random
from collections.abc import Sequence
from typing import Protocol

from ..models import Priority


class PriorityPolicy(Protocol):
    def choose(self, active_priorities: Sequence[Priority]) -> Priority:
        """Choose one currently active priority."""


class AbsolutePriorityPolicy:
    """Always choose the highest-priority active level."""

    def choose(self, active_priorities: Sequence[Priority]) -> Priority:
        return min(active_priorities)


class RandomPriorityPolicy:
    """Randomly choose an active priority, favoring higher priorities."""

    def choose(self, active_priorities: Sequence[Priority]) -> Priority:
        ordered_priorities = sorted(active_priorities)
        priority_weights = {
            priority: len(ordered_priorities) - index
            for index, priority in enumerate(ordered_priorities)
        }
        weights = [priority_weights[priority] for priority in active_priorities]
        return random.choices(active_priorities, weights=weights, k=1)[0]