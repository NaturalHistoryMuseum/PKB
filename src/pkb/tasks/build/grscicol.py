"""Compatibility imports for GRSciColl institution build tasks.

New code should import from ``pkb.tasks.build.institutions.grscicol``.
"""

from pkb.tasks.build.institutions.grscicol import (
    GRSciCollAPITask,
    GRSciCollCollectionsAPITask,
    GRSciCollCollectionsTSVTask,
    GRSciCollInstitutionsAPITask,
    GRSciCollInstitutionsTSVTask,
)

__all__ = [
    "GRSciCollAPITask",
    "GRSciCollCollectionsAPITask",
    "GRSciCollCollectionsTSVTask",
    "GRSciCollInstitutionsAPITask",
    "GRSciCollInstitutionsTSVTask",
]
