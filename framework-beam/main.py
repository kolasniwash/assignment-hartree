import dataclasses
import typing
from dataclasses import dataclass
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.transforms.combiners import CountCombineFn
import apache_beam as beam

pipeline = beam.Pipeline()

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


def hash_key(x):
    key = "".join([x.legal_entity, str(x.tier)])
    return hash(key), x


def build_output_object(base, accr, arac):

    return Output(
        legal_entity=base[0],
        counter_party=base[1],
        tier=base[2],
        max_rating_by_counterparty=base[3],
        sum_of_ARAP=arac[0][-1] if len(arac) > 0 else 0,
        sum_of_ACCR=accr[0][-1] if len(accr) > 0 else 0
    )


schema = pyarrow.schema(
    [("legal_entity", pyarrow.string()),
     ("counter_party", pyarrow.string()),
     ("tier", pyarrow.int64()),
     ("max_rating_by_counterparty", pyarrow.int64()),
     ("sum_of_ARAP", pyarrow.int64()),
     ("sum_of_ACCR", pyarrow.int64())]
)


def run():

    with pipeline as p:

        d1 = (
                p
                | "read_d1_input" >> ReadFromText("../data/dataset1.csv")
                | "read_d1_lines" >> beam.FlatMap(lambda x: x.split("\r")[1:])
                | "parse_d1" >> beam.Map(parse_entities)
        )


        d2 = (
                p
                | "read_d2_input" >> ReadFromText("../data/dataset2.csv")
                | "read_d2_lines" >> beam.FlatMap(lambda x: x.split("\r")[1:])
                | "parse_d2" >> beam.Map(parse_counter_parties)
        )

        merged = d1 | "join_d2" >> beam.ParDo(EnrichD1(), beam.pvalue.AsDict(d2))

        group_base = (
                merged
                | "base_grouping" >> beam.GroupBy(legal_entity="legal_entity", tier="tier")
                                            .aggregate_field("counter_party", CountCombineFn(), "total")
                                            .aggregate_field("rating", max, "max_rating")
                | "make key2" >> beam.Map(lambda x: hash_key(x))

        )

        filter_arap = (
                merged
                | "filter_ARAP" >> beam.Filter(lambda x: x.status == "ARAP")
                | "grouper_arap" >> beam.GroupBy(legal_entity="legal_entity", tier="tier").aggregate_field("value", sum, "sum_of_arap")
                | "make key3" >> beam.Map(lambda x: hash_key(x))
        )
        #
        filter_accr = (
                merged
                | "filter_ACCR" >> beam.Filter(lambda x: x.status == "ACCR")
                | "grouper_accr" >> beam.GroupBy(legal_entity="legal_entity", tier="tier").aggregate_field("value", sum, "sum_of_accr")
                | "make key" >> beam.Map(lambda x: hash_key(x))
        )
        #
        table = (
            ({"b": group_base, "ac": filter_accr, "ar": filter_arap})
            | "combine" >> beam.CoGroupByKey()
            | "mappp" >> beam.Map(lambda x : build_output_object(x[1]["b"][0], x[1]["ac"], x[1]["ar"]))
            | "asdict" >> beam.Map(dataclasses.asdict)
        )

        existing = p | "read_existing_input" >> ReadFromText("./final_output.csv")

        output = (
            (existing, table)
            | beam.Flatten()
            | "to parquet" >> beam.io.WriteToText("./final_output",
                                                file_name_suffix='.csv',
                                                append_trailing_newlines=True,
                                                shard_name_template='')
        )

if __name__ == "__main__":
    run()

