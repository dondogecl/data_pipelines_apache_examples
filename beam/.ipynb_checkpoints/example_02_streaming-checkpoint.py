import apache_beam as beam
from apache_beam.testing.test_stream import TestStream
from apache_beam.coders import StrUtf8Coder
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode
from apache_beam import window  # for TimestampedValue
import datetime

# toy data (like your 'lines' list)
lines = ["lorem ipsum", "dolor sit amet", "lorem lorem"]

class Tokenize(beam.PTransform):
    """
    A PTransform that tokenizes lines of text into individual words.

    It takes a PCollection of strings as input, where each string represents a line of text.
    It returns a PCollection of strings, where each string represents a word.
    """
    def expand(self, pcoll):
        """
        Applies the tokenization logic to the input PCollection.

        Args:
            pcoll: A PCollection of strings, where each string represents a line of text.

        Returns:
            A PCollection of strings, where each string represents a word.
        """
        return pcoll | "Split words" >> beam.FlatMap(lambda s: s.split())

def build_stream(lines):
    # Start “now”, emit each line 1 second apart
    start = datetime.datetime.utcnow()
    ts = TestStream(coder=StrUtf8Coder())
    for i, line in enumerate(lines):
        ts = ts.add_elements([
            window.TimestampedValue(line, timestamp=start.timestamp() + i)
        ])
    # Let the watermark run to the end so triggers can fire
    return ts.advance_watermark_to_infinity()

options = PipelineOptions([])  # DirectRunner by default

with beam.Pipeline(options=options) as p:
    # 1) Unbounded input from TestStream
    input_lines = p | "TestStream" >> build_stream(lines)

    # 2) (Optional) This would FAIL without windowing (uncomment to see):
    # (input_lines
    #  | "Tokenize_BAD" >> Tokenize()
    #  | "Count_BAD" >> beam.combiners.Count.PerElement()
    #  | "Print_BAD" >> beam.Map(print))

    # 3) Windowing + trigger to make GBK legal on an unbounded PCollection
    windowed = (
        input_lines
        | "GlobalWindowsWithTrigger" >> beam.WindowInto(
            GlobalWindows(),
            trigger=AfterWatermark(),                 # fire when watermark passes end-of-window
            accumulation_mode=AccumulationMode.DISCARDING
        )
    )

    # 4) Now aggregations like Count/GBK are allowed
    (
        windowed
        | "Tokenize" >> Tokenize()
        | "Count" >> beam.combiners.Count.PerElement()
        | "Print" >> beam.Map(print)                 # e.g. ('lorem', 3)
    )
