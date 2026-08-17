"""Compatibility imports for Wikidata build tasks.

New code should import from the owning knowledge-domain package, for example
``pkb.tasks.build.people.wikidata`` or ``pkb.tasks.build.institutions.wikidata``.
"""

from pkb.tasks.build.institutions.wikidata import (
    WikiDataInstitutionIDsTask,
    WikiDataInstitutionsTask,
)
from pkb.tasks.build.people.wikidata import (
    WikiDataCollectorIDsTask,
    WikiDataCollectorsTask,
    WikiDataPeopleTask,
    collect_related_qids_for_collectors,
)

__all__ = [
    "WikiDataCollectorIDsTask",
    "WikiDataCollectorsTask",
    "WikiDataPeopleTask",
    "WikiDataInstitutionIDsTask",
    "WikiDataInstitutionsTask",
    "collect_related_qids_for_collectors",
]
