"""Compatibility imports for Wikidata build tasks.

New code should import from the owning knowledge-domain package, for example
``pkb.tasks.build.people.wikidata`` or ``pkb.tasks.build.institutions.wikidata``.
"""

from pkb.tasks.build.institutions.wikidata import (
    WikiDataInstitutionsTask,
)
from pkb.tasks.build.people.wikidata import WikiDataPeopleTask

__all__ = [
    "WikiDataPeopleTask",
    "WikiDataInstitutionsTask",
]
