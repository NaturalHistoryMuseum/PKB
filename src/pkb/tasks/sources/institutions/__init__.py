"""Institution and collection source materialisation tasks."""

from pkb.tasks.sources.institutions.grscicol import (
    GRSciCollCollectionsTSVSourceTask,
    GRSciCollInstitutionsTSVSourceTask,
)
from pkb.tasks.sources.institutions.wikidata import (
    WikiDataInstitutionEntityBatchSourceTask,
    WikiDataInstitutionEntitiesSourceTask,
    WikiDataInstitutionIDsSourceTask,
)

__all__ = [
    "GRSciCollCollectionsTSVSourceTask",
    "GRSciCollInstitutionsTSVSourceTask",
    "WikiDataInstitutionEntityBatchSourceTask",
    "WikiDataInstitutionEntitiesSourceTask",
    "WikiDataInstitutionIDsSourceTask",
]
