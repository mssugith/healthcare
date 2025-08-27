import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class ProcessHealthcareData(beam.DoFn):
    def process(self, element):
        # Add data cleaning/transformation logic
        yield element

def run():
    options = PipelineOptions(
        project='healthcare-analytics-sugith',
        runner='DataflowRunner',
        region='asia-south1-a',
        zone='asia-south1-a',
        temp_location='gs://healthcare-raw-data-healthcare-analytics-sugith/temp /'
    )
  
    with beam.Pipeline(options=options) as p:
        (p
         | 'ReadFromGCS' >> beam.io.ReadFromText('gs://healthcare-raw-data-healthcare-analytics-sugith/Hdataset/heart_disease_risk_dataset_earlymed2.csv')
         | 'ParseCSV' >> beam.Map(lambda line: line.split(','))
         | 'ProcessData' >> beam.ParDo(ProcessHealthcareData())
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             'healthcare-analytics-sugith:patient_details.patients_factors_2',
             schema='Chest_Pain: BINARY, Shortness_of_Breath: BINARY,	Fatigue: BINARY,	Palpitations: BINARY,	Dizziness: BINARY,	Swelling: BINARY, Age: INT, Heart_Risk: BINARY ',
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )

if __name__ == '__main__':
    run()
