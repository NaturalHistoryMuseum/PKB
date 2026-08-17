"""Compatibility imports for Harvard Index people build tasks.

New code should import from ``pkb.tasks.build.people.harvard``.
"""

from pkb.tasks.build.people.harvard import HarvardAgentsFile, HarvardIndexDetailTask

__all__ = [
    "HarvardAgentsFile",
    "HarvardIndexDetailTask",
]
