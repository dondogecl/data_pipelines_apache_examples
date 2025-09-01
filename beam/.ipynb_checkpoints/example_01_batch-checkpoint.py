import apache_beam as beam

# Simple tokenizer as a Beam PTransform
class Tokenize(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | "Split into words" >> beam.FlatMap(lambda line: line.split())
        )

# File path
file_path = "data/lorem.txt"

# Define the pipeline
with beam.Pipeline() as pipeline:
    (
        pipeline
        | "Read lines" >> beam.io.ReadFromText(file_path)
        | "Tokenize" >> Tokenize()
        | "Count words" >> beam.combiners.Count.PerElement()
        | "Print results" >> beam.Map(print)
    )
