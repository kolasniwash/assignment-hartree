import os.path
import dataclasses
from pathlib import Path

import apache_beam as beam
from apache_beam.io import ReadFromText
from utils import (parse_entities,
                   parse_counter_parties,
                   EnrichD1,
                   hash_key,
                   build_output_object,
                   get_group_and_out_of_group,
                   base_group,
                   build_grouper)

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

    # Pipeline is NOT idempotent. Overwrite output as idempotent process
    if os.path.exists("final-output.csv"):
        os.remove("final-output.csv")
    Path("final-output.csv").touch()

    for group in grouping_sets:
        run(group)

