"""People source materialisation tasks."""

from pkb.tasks.sources.people.wikidata import (
    WikiDataCollectorEntitiesSourceTask,
    WikiDataCollectorIDsSourceTask,
    WikiDataCollectorPropertyEntitiesSourceTask,
)

__all__ = [
    "WikiDataCollectorEntitiesSourceTask",
    "WikiDataCollectorIDsSourceTask",
    "WikiDataCollectorPropertyEntitiesSourceTask",
]
