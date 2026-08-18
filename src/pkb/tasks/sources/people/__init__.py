"""People source materialisation tasks."""

from pkb.tasks.sources.people.bionomia import BionomiaPublicProfilesSourceTask
from pkb.tasks.sources.people.wikidata import (
    WikiDataCollectorEntitiesSourceTask,
    WikiDataCollectorIDsSourceTask,
    WikiDataCollectorPropertyEntitiesSourceTask,
)

__all__ = [
    "BionomiaPublicProfilesSourceTask",
    "WikiDataCollectorEntitiesSourceTask",
    "WikiDataCollectorIDsSourceTask",
    "WikiDataCollectorPropertyEntitiesSourceTask",
]
