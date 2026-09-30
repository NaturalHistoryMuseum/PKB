"""People-domain aggregation and resolution models."""

from pkb.models.people.aggregation import PersonAggregationResult, PersonAggregator
from pkb.models.people.huh_deduplication import (
    HUHDeduplicationResult,
    HUHPersonDeduplicator,
)
from pkb.models.people.names import (
    ParsedPersonName,
    parse_person_name,
    person_names_compatible,
    person_names_match_by_given_names,
    person_names_match_by_initials,
)
from pkb.models.people.resolution import (
    build_people_identifier_map,
    match_people_by_identifier,
    match_people_by_logistic_regression,
)

__all__ = [
    "ParsedPersonName",
    "HUHDeduplicationResult",
    "HUHPersonDeduplicator",
    "PersonAggregationResult",
    "PersonAggregator",
    "build_people_identifier_map",
    "parse_person_name",
    "match_people_by_identifier",
    "match_people_by_logistic_regression",
    "person_names_compatible",
    "person_names_match_by_given_names",
    "person_names_match_by_initials",
]
