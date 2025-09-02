import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class DataflowOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument("--input", type=str, help="GCS input file")
        parser.add_value_provider_argument("--output", type=str, help="BigQuery table")


def transform_data(element):
    fields = element.split(",")
    return {
        "Chest_Pain": fields[0],
        "Shortness_of_Breath": fields[1],
        "Age": int(fields[17]) if fields[17].isdigit() else None,
        "Heart_Risk": fields[18],
    }


def run():
    pipeline_options = PipelineOptions(
        save_main_session=True,
        runner="DirectRunner",  # local test
        project="healthcare-analytics-sugith",
        region="asia-south1",   # region not zone
        temp_location="gs://healthcare-processed-data-healthcare-analytics-sugith/temp/",
        staging_location="gs://healthcare-processed-data-healthcare-analytics-sugith/staging/",
    )

    options = pipeline_options.view_as(DataflowOptions)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read from GCS" >> beam.io.ReadFromText(options.input, skip_header_lines=1)
            | "Transform" >> beam.Map(transform_data)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=options.output,
                schema="Chest_Pain:STRING, Shortness_of_Breath:STRING, Age:INTEGER, Heart_Risk:STRING",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if _name_ == "__main__":
    run()
