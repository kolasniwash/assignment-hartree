import typing
from dataclasses import dataclass


@dataclass
class Entities:
    invoice_id: int
    legal_entity: str
    counter_party: str
    rating: int
    status: str
    value: int

class CounterParties(typing.NamedTuple):
    counter_party: str
    tier: int


@dataclass
class JoinDataset:
    invoice_id: int
    legal_entity: str
    counter_party: str
    rating: int
    status: str
    value: int
    tier: int


@dataclass
class Output:
    legal_entity: str
    counter_party: str
    tier: int
    max_rating_by_counterparty: int
    sum_of_ARAP: int
    sum_of_ACCR: int