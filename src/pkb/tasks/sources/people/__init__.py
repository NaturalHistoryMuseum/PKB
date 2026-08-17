"""People source materialisation tasks."""

from pkb.tasks.sources.people.wikidata import (
    WikiDataCollectorEntitiesSourceTask,
    WikiDataCollectorEntityBatchSourceTask,
    WikiDataCollectorIDsSourceTask,
    WikiDataRelatedCollectorEntitiesSourceTask,
    WikiDataRelatedCollectorEntityBatchSourceTask,
)

__all__ = [
    "WikiDataCollectorEntitiesSourceTask",
    "WikiDataCollectorEntityBatchSourceTask",
    "WikiDataCollectorIDsSourceTask",
    "WikiDataRelatedCollectorEntitiesSourceTask",
    "WikiDataRelatedCollectorEntityBatchSourceTask",
]
