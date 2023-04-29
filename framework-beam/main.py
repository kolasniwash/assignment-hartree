from dataclasses import dataclass

from apache_beam.dataframe import io
from apache_beam.io import ReadFromText, WriteToText
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
@dataclass
class CounterParties:
    counter_party: str
    tier: int


def parse_entities(elements):
    element = elements.split(',')
    return Entities(
            int(element[0]),
            element[1],
            element[2],
            int(element[3]),
            element[4],
            int(element[5])
    )


def parse_counter_parties(elements):
    element = elements.split(',')
    return CounterParties(element[0], int(element[1]))


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


        d1 | 'Write_d1' >> WriteToText("./d1-pcollection")
        d1 | "print_d1">> beam.Map(print)

        d2 | 'Write' >> WriteToText("./d2-pcollection")
        d2 | "print" >> beam.Map(print)


if __name__ == "__main__":
    run()

