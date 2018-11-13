"""
To specify a different runner:
  --runner YOUR_RUNNER

NOTE: When specifying a different runner, additional runner-specific options
      may have to be passed in as well

EXAMPLES
--------

# DirectRunner
python high_values.py \
    --project $PROJECT_ID \
    --topic projects/$PROJECT_ID/topics/$PUBSUB_TOPIC \
    --dataset $BIGQUERY_DATASET

# DataflowRunner
python high_values.py \
    --project $PROJECT_ID \
    --topic projects/$PROJECT_ID/topics/$PUBSUB_TOPIC \
    --dataset $BIGQUERY_DATASET \
    --runner DataflowRunner \
    --temp_location gs://$BUCKET/user_value/temp
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import csv
import logging
import sys
import time
from datetime import datetime

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import trigger


def timestamp2str(t, fmt='%Y-%m-%d %H:%M:%S.000'):
    """Converts a unix timestamp into a formatted string."""
    return datetime.fromtimestamp(t).strftime(fmt)


class ParseEventFn(beam.DoFn):
    """Parses the raw game event info into a Python dictionary.

    Each event line has the following format:
        username,group,value,timestamp_in_ms,readable_time

    e.g.:
        user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224

    The human-readable time string is not used here.
    """
    def __init__(self):
        super(ParseEventFn, self).__init__()
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):
        logging.info(40 * "$")
        logging.info('ELEM: {}'.format(elem))
        logging.info(40 * "$")

        try:
            row = list(csv.reader([elem]))[0]
            logging.info(row)
            yield {
                'user': row[0],
                'group': row[1],
                'value': int(row[2]),
                'timestamp': int(row[3]) / 1000.0,
            }
        except:  # pylint: disable=bare-except
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', elem)


class ExtractAndSumValue(beam.PTransform):
    """A transform to extract key/value information and sum the values.
    The constructor argument `field` determines whether 'group' or 'user' info is
    extracted.
    """
    def __init__(self, field):
        super(ExtractAndSumValue, self).__init__()
        self.field = field

    def expand(self, pcoll):
        logging.info("Extract sum and value.")
        return (pcoll
                | beam.Map(lambda elem: (elem[self.field], elem['value']))
                | beam.CombinePerKey(sum))


class GroupValuesDict(beam.DoFn):
    """Formats the data into a dictionary of BigQuery columns with their values

    Receives a (group, value) pair, extracts the window start timestamp, and
    formats everything together into a dictionary. The dictionary is in the format
    {'bigquery_column': value}
    """
    def process(self, group_value, window=beam.DoFn.WindowParam):
        group, value = group_value
        start = timestamp2str(int(window.start))
        logging.info("Group values: {} and start: {}.".format(group_value,
                                                              start))
        yield {
            'group': group,
            'total_value': value,
            'window_start': start,
            'processing_time': timestamp2str(int(time.time()))
        }


class WriteToBigQuery(beam.PTransform):
    """Generate, format, and write BigQuery table row information."""
    def __init__(self, table_name, dataset, schema, project):
        """Initializes the transform.
        Args:
        table_name: Name of the BigQuery table to use.
        dataset: Name of the dataset to use.
        schema: Dictionary in the format {'column_name': 'bigquery_type'}
        project: Name of the Cloud project containing BigQuery table.
        """
        super(WriteToBigQuery, self).__init__()
        logging.info("BIG QUERY Table: {}, Dataset: {}".format(table_name, dataset))
        self.table_name = table_name
        self.dataset = dataset
        self.schema = schema
        self.project = project

    def get_schema(self):
        """Build the output table schema."""
        logging.info("Get schema")
        return ', '.join(
            '%s:%s' % (col, self.schema[col]) for col in self.schema)

    def expand(self, pcoll):
        logging.info("Writing to big query: {}".format(pcoll))

        return (
            pcoll
            | 'ConvertToRow' >> beam.Map(
                lambda elem: {col: elem[col] for col in self.schema})
            | beam.io.WriteToBigQuery(
                self.table_name, self.dataset, self.project, self.get_schema()))


class CalculateGroupValues(beam.PTransform):
    """Calculates values for each group within the configured window duration.

    Extract group/value pairs from the event stream, using hour-long windows by
    default.
    """
    def __init__(self, group_window_duration, allowed_lateness):
        super(CalculateGroupValues, self).__init__()
        self.group_window_duration = group_window_duration * 60
        self.allowed_lateness_seconds = allowed_lateness * 60

    def expand(self, pcoll):
        logging.info("Calculate group values: {}".format(pcoll))

        return (
            pcoll
            # We will get early (speculative) results as well as cumulative
            # processing of late data.
            | 'HighValueGroupFixedWindows' >> beam.WindowInto(
                beam.window.FixedWindows(self.group_window_duration),
                trigger=trigger.AfterWatermark(trigger.AfterCount(10),
                                               trigger.AfterCount(20)),
                accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
            # Extract and sum group/value pairs from the event data.
            | 'ExtractAndSumValue' >> ExtractAndSumValue('group'))


class CalculateUserValues(beam.PTransform):
    """Extract user/value pairs from the event stream using processing time, via
    global windowing. Get periodic updates on all users' running values.
    """
    def __init__(self, allowed_lateness):
        super(CalculateUserValues, self).__init__()
        self.allowed_lateness_seconds = allowed_lateness * 60

    def expand(self, pcoll):
        logging.info("Calculate user values: {}".format(pcoll))

        return (
            pcoll
            # Get periodic results every ten events.
            | 'HighValueUserGlobalWindows' >> beam.WindowInto(
                beam.window.GlobalWindows(),
                trigger=trigger.Repeatedly(trigger.AfterCount(10)),
                accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
            # Extract and sum username/value pairs from the event data.
            | 'ExtractAndSumValue' >> ExtractAndSumValue('user'))


def run(argv=None):
    """Main entry point; defines and runs the hourly_team_value pipeline."""
    parser = argparse.ArgumentParser()

    parser.add_argument('--topic',
                        type=str,
                        help='Pub/Sub topic to read from')
    parser.add_argument('--subscription',
                        type=str,
                        help='Pub/Sub subscription to read from')
    parser.add_argument('--dataset',
                        type=str,
                        required=True,
                        help='BigQuery Dataset to write tables to. '
                        'Must already exist.')
    parser.add_argument('--table_name',
                        default='high_values',
                        help='The BigQuery table name. Should not already exist.')
    parser.add_argument('--group_window_duration',
                        type=int,
                        default=60,
                        # default=360,
                        help='Numeric value of fixed window duration for group '
                        'analysis, in minutes')
    parser.add_argument('--allowed_lateness',
                        type=int,
                        default=120,
                        # default=720,
                        help='Numeric value of allowed data lateness, in minutes')

    args, pipeline_args = parser.parse_known_args(argv)

    logging.info(40 * "#")
    logging.info(datetime.now())
    logging.info(40 * "#")

    if args.topic is None and args.subscription is None:
        parser.print_usage()
        print(sys.argv[0] + ': error: one of --topic or --subscription is required')
        sys.exit(1)

    options = PipelineOptions(pipeline_args)

    # We also require the --project option to access --dataset
    if options.view_as(GoogleCloudOptions).project is None:
        parser.print_usage()
        print(sys.argv[0] + ': error: argument --project is required')
        sys.exit(1)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    options.view_as(SetupOptions).save_main_session = True

    # Enforce that this pipeline is always run in streaming mode
    options.view_as(StandardOptions).streaming = True

    p = beam.Pipeline(options=options)

    # Read from PubSub into a PCollection.
    if args.subscription:
        values = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(
            subscription=args.subscription)
    else:
        values = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(
            topic=args.topic)

    events = (
        values
        | 'ParseEventFn' >> beam.ParDo(ParseEventFn())
        | 'AddEventTimestamps' >> beam.Map(
            lambda elem: beam.window.TimestampedValue(elem, elem['timestamp'])))

    # Get group values and write the results to BigQuery
    (events  # pylint: disable=expression-not-assigned
        | 'CalculateGroupValues' >> CalculateGroupValues(
            args.group_window_duration, args.allowed_lateness)
        | 'GroupValuesDict' >> beam.ParDo(GroupValuesDict())
        | 'WriteGroupValueSums' >> WriteToBigQuery(
            args.table_name + '_groups', args.dataset, {
                'group': 'STRING',
                'total_value': 'INTEGER',
                'window_start': 'STRING',
                'processing_time': 'STRING',
            }, options.view_as(GoogleCloudOptions).project))

    def format_user_value_sums(user_value):
        (user, value) = user_value
        t = timestamp2str(int(time.time()))
        return {'user': user, 'total_value': value, 'update_time': t}

    # Get user values and write the results to BigQuery
    (events  # pylint: disable=expression-not-assigned
        | 'CalculateUserValues' >> CalculateUserValues(args.allowed_lateness)
        | 'FormatUserValueSums' >> beam.Map(format_user_value_sums)
        | 'WriteUserValueSums' >> WriteToBigQuery(
            args.table_name + '_users', args.dataset, {
                'user': 'STRING',
                'total_value': 'INTEGER',
                'update_time': 'STRING',
            }, options.view_as(GoogleCloudOptions).project))

    p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
