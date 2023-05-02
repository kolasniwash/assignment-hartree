from typing import List, Tuple, Generator

import apache_beam as beam
import dataclasses

from apache_beam.pvalue import PCollection
from apache_beam.transforms.combiners import CountCombineFn
from schemas import Entities, Output

def parse_entities(elements: str):
    """
    Parse string to structured dictionary.
    :param elements:
    :return:
    """
    element = elements.split(',')
    return dataclasses.asdict(
        Entities(
            int(element[0]),
            element[1],
            element[2],
            int(element[3]),
            element[4],
            int(element[5])
        )
    )


def parse_counter_parties(elements: str) -> Tuple[str, int]:
    """
    Parse string to tuple.
    :param elements:
    :return:
    """
    element = elements.split(',')
    return (element[0], int(element[1]))


class EnrichD1(beam.DoFn):
    def process(self, element: PCollection, counter_party_lookup: dict) -> Generator[beam.Row, None, None]:
        """
        Join side input dict to main PCollection.
        :param element:
        :param counter_party_lookup:
        :return:
        """
        tier = counter_party_lookup.get(element["counter_party"], None)
        element["tier"] = tier
        yield beam.Row(**element)

def hash_key(x: PCollection, group_dict: dict) -> Tuple[int, PCollection]:
    """
    Hash the grouping set.
    :param x:
    :param group_dict:
    :return:
    """

    hash_key_list = [str(x[i]) for i, item in enumerate(group_dict.values())]
    key = "".join(hash_key_list)
    return hash(key), x


def build_output_object(base: PCollection, accr: PCollection, arac: PCollection) -> Output:
    """
    Make named dictionary from PCollections.
    :param base:
    :param accr:
    :param arac:
    :return:
    """

    return Output(
        legal_entity=base.legal_entity,
        counter_party=base.counter_party,
        tier=base.tier,
        max_rating_by_counterparty=base.max_rating,
        sum_of_ARAP=arac[0][-1] if len(arac) > 0 else 0,
        sum_of_ACCR=accr[0][-1] if len(accr) > 0 else 0
    )

def get_group_and_out_of_group(group_set: List[str]) -> Tuple[dict, List[str]]:
    """
    Make dictionary of group items and out of group items.

    :param group_set:
    :return:
    """
    grouping_set_items = {"legal_entity", "counter_party", "tier"}
    out_of_group_items = set(group_set).symmetric_difference(grouping_set_items)
    group_dict = {v: v for v in group_set}

    return group_dict, list(out_of_group_items)


def base_group(group_dict: dict) -> beam.GroupBy:
    """
    Expand group dict into arguments for beam group by.
    :param group_dict:
    :return:
    """
    return beam.GroupBy(**group_dict)

def build_grouper(group_dict: dict, out_of_group: List[str]) -> beam.GroupBy:
    """
    Make grouping set object based on number of out of group items.

    :param group_dict:
    :param out_of_group:
    :return: beam.Groupby
    """
    if len(out_of_group) == 0:
        return base_group(group_dict)
    elif len(out_of_group) == 1:
        return base_group(group_dict)\
            .aggregate_field(out_of_group[0], CountCombineFn(), out_of_group[0])
    elif len(out_of_group) == 2:
        return base_group(group_dict) \
            .aggregate_field(out_of_group[0], CountCombineFn(), out_of_group[0])\
            .aggregate_field(out_of_group[1], CountCombineFn(), out_of_group[1])
    else:
        raise ValueError("Error: out of group size max 2.")
