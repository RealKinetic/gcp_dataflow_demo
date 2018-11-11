from __future__ import absolute_import
from __future__ import division

import argparse
import csv
import logging

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions


class ParseRowFn(beam.DoFn):
    def __init__(self):
        super(ParseRowFn, self).__init__()
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):
        try:
            logging.info("RECORD")
            logging.info(elem)
            row = list(csv.reader([elem]))[0]
            yield {
                'user': row[0],
                'value': float(row[1]),
                'timestamp': row[2],
            }
        except Exception as e:  # pylint: disable=bare-except
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error(e)
            logging.error('Parse error on "%s"', elem)


class ExtractAndSumValues(beam.PTransform):
    def __init__(self, field):
        super(ExtractAndSumValues, self).__init__()
        self.field = field

    def expand(self, pcoll):
        return (pcoll
                | beam.Map(lambda elem: (elem[self.field], elem['value']))
                | beam.CombinePerKey(sum))


class UserAggregate(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | 'ParseRowFn' >> beam.ParDo(ParseRowFn())
            | 'ExtractAndSumValues' >> ExtractAndSumValues('user'))


def format_user_score_sums(user_score):
    (user, score) = user_score
    return 'user: %s, total_score: %s' % (user, score)


def run():
    options = PipelineOptions()

    input_ = 'gs://tempgcpbucket1/entries.csv'
    output_ = 'gs://tempgcpbucket1/counts/'
    # options.input = input_
    # options.output = output_

    options = options.view_as(GoogleCloudOptions)
    options.project = 'rk-playground'
    options.job_name = 'entriesjob'
    options.staging_location = 'gs://tempgcpbucket1/binaries'
    options.temp_location = 'gs://tempgcpbucket1/tmp'

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    options.view_as(SetupOptions).save_main_session = True

    options.view_as(StandardOptions).runner = 'DataflowRunner'

    p = beam.Pipeline(options=options)

    (p  # pylint: disable=expression-not-assigned
        | 'ReadInputText' >> beam.io.ReadFromText(input_)
        | 'UserAggregate' >> UserAggregate()
        | 'FormatUserScoreSums' >> beam.Map(format_user_score_sums)
        | 'WriteUserScoreSums' >> beam.io.WriteToText(output_))

    p.run()


def run_entries_flow():
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Running data flow job.")
    run()


if __name__ == '__main__':
    run_entries_flow()
