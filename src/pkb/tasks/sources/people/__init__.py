"""People source materialisation tasks."""

from pkb.tasks.sources.people.bionomia import BionomiaPublicProfilesSourceTask
from pkb.tasks.sources.people.harvard import (
    HarvardIndexAgentsFile,
    HarvardIndexCollectorsFile,
    HarvardIndexCollectorsSourceTask,
)
from pkb.tasks.sources.people.wikidata import (
    WikiDataCollectorEntitiesSourceTask,
    WikiDataCollectorIDsSourceTask,
    WikiDataCollectorPropertyIDsTask,
    WikiDataCollectorPropertyEntitiesSourceTask,
)

__all__ = [
    "BionomiaPublicProfilesSourceTask",
    "HarvardIndexAgentsFile",
    "HarvardIndexCollectorsFile",
    "HarvardIndexCollectorsSourceTask",
    "WikiDataCollectorEntitiesSourceTask",
    "WikiDataCollectorIDsSourceTask",
    "WikiDataCollectorPropertyEntitiesSourceTask",
    "WikiDataCollectorPropertyIDsTask",
]
