import argparse
import os.path
import typing
import dataclasses
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.transforms.combiners import CountCombineFn




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

def parse_entities(elements):
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


def parse_counter_parties(elements):
    element = elements.split(',')
    return (element[0], int(element[1]))


class EnrichD1(beam.DoFn):
    def process(self, element, counter_party_lookup):
        tier = counter_party_lookup.get(element["counter_party"], None)
        element["tier"] = tier
        yield beam.Row(**element)

def join_func(left , right):
    if left.legal_entity == right.legal_entity and right.tier == right.tier:
        return Output(sum_of_ACCR=right.get("sum_of_ACCR"), **left)


def hash_key(x, group_dict):

    hash_key_list = [str(x[i]) for i, item in enumerate(group_dict.values())]
    key = "".join(hash_key_list)
    return hash(key), x


def build_output_object(base, accr, arac):

    return Output(
        legal_entity=base.legal_entity,
        counter_party=base.counter_party,
        tier=base.tier,
        max_rating_by_counterparty=base.max_rating,
        sum_of_ARAP=arac[0][-1] if len(arac) > 0 else 0,
        sum_of_ACCR=accr[0][-1] if len(accr) > 0 else 0
    )

def get_group_and_out_of_group(group_set):

    grouping_set_items = {"legal_entity", "counter_party", "tier"}
    out_of_group_items = set(group_set).symmetric_difference(grouping_set_items)
    group_dict = {v: v for v in group_set}

    return group_dict, list(out_of_group_items)


def base_group(group_dict):
    return beam.GroupBy(**group_dict)

def build_grouper(group_dict, out_of_group):

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

def run(group):

    pipeline = beam.Pipeline()


    with pipeline as p:

        d1 = (
                p
                | f"read_d1_input_{group}" >> ReadFromText("../data/dataset1.csv")
                | f"read_d1_lines_{group}" >> beam.FlatMap(lambda x: x.split("\r")[1:])
                | f"parse_d1_{group}" >> beam.Map(parse_entities)
        )

        d2 = (
                p
                | f"read_d2_input_{group}" >> ReadFromText("../data/dataset2.csv")
                | f"read_d2_lines_{group}" >> beam.FlatMap(lambda x: x.split("\r")[1:])
                | f"parse_d2_{group}" >> beam.Map(parse_counter_parties)
        )

        merged = d1 | f"join_d2_{group}" >> beam.ParDo(EnrichD1(), beam.pvalue.AsDict(d2))

        group_dict, out_of_group = get_group_and_out_of_group(group)

        group_base = (
                merged
                | f"base_grouping_{group}" >> build_grouper(group_dict, out_of_group).aggregate_field("rating", max, "max_rating")
                | f"make key2_{group}" >> beam.Map(lambda x: hash_key(x, group_dict))

        )

        filter_arap = (
                merged
                | f"filter_ARAP_{group}" >> beam.Filter(lambda x: x.status == "ARAP")
                | f"grouper_arap_{group}" >> base_group(group_dict).aggregate_field("value", sum, "sum_of_arap")
                | f"make key3_{group}" >> beam.Map(lambda x: hash_key(x, group_dict))
        )
        #
        filter_accr = (
                merged
                | f"filter_ACCR_{group}" >> beam.Filter(lambda x: x.status == "ACCR")
                | f"grouper_accr_{group}" >> base_group(group_dict).aggregate_field("value", sum, "sum_of_accr")
                | f"make key_{group}" >> beam.Map(lambda x: hash_key(x, group_dict))
        )
        # #
        table = (
            ({"b": group_base, "ac": filter_accr, "ar": filter_arap})
            | f"combine_{group}" >> beam.CoGroupByKey()
            | f"mappp_{group}" >> beam.Map(lambda x: build_output_object(x[1]["b"][0], x[1]["ac"], x[1]["ar"]))
            | f"asdict_{group}" >> beam.Map(dataclasses.asdict)
        )

        existing = p | f"read_existing_input_{group}" >> ReadFromText("final-output.csv")

        output = (
            (existing, table)
            | f"flatten_{group}" >> beam.Flatten()
            | f"to text_{group}" >> beam.io.WriteToText(f"final-output",
                                                file_name_suffix='.csv',
                                                append_trailing_newlines=True,
                                                shard_name_template="")
        )

if __name__ == "__main__":

    grouping_sets = [
        ["legal_entity"],
        ["legal_entity", "counter_party"],
        ["legal_entity", "counter_party", "tier"],
        ["tier"],
        ["counter_party"]
    ]

    print(os.getcwd())
    if os.path.exists("final-output.csv"):
        os.remove("final-output.csv")
    Path("final-output.csv").touch()

    for group in grouping_sets:
        run(group)

