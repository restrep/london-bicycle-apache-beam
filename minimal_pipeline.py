# minimal_pipeline.py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from geopy.distance import geodesic

PROJECT_ID = "london-bicycle-apache-beam"
BUCKET_NAME = "ml6-test-london-bikes"
OUTPUT_PATH = f'gs://{BUCKET_NAME}/output'


def test_geodesic(element):
    point1 = (52.5200, 13.4050)
    point2 = (51.5074, 0.1278)
    distance = geodesic(point1, point2).km
    return distance

project_options = PipelineOptions(
    runner='DataflowRunner',
    project=f"{PROJECT_ID}",  # Your project ID
    job_name='minimal-test',
    temp_location=f'gs://{BUCKET_NAME}/temp',
    region='europe-west10',
    setup_file='./setup.py',
    requirements_file='./requirements.txt' # keep this for local
)

with beam.Pipeline(options=project_options) as p:
    lines = p | 'Create' >> beam.Create(['test'])
    result = lines | 'Test Geodesic' >> beam.Map(test_geodesic)
    result | 'Print' >> beam.Map(print)