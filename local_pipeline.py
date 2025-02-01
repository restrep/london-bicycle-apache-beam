import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from geopy.distance import geodesic

# Sample test data to test locally
stations_data = [
    {'id': 1, 'latitude': 51.5, 'longitude': -0.1},
    {'id': 2, 'latitude': 51.52, 'longitude': -0.12},
    {'id': 3, 'latitude': 51.49, 'longitude': -0.11}
]

cycle_hire_data = [
    {'start_station_id': 1, 'end_station_id': 2},
    {'start_station_id': 1, 'end_station_id': 2},
    {'start_station_id': 1, 'end_station_id': 3},
    {'start_station_id': 2, 'end_station_id': 3},
    {'start_station_id': 3, 'end_station_id': 1},
    {'start_station_id': 3, 'end_station_id': 1}
]


# Define the pipeline
def run_pipeline():
    options = PipelineOptions(
        runner='DirectRunner'  # this is uset to run locally. On GCP we would use DataFlow
    )
    
    with beam.Pipeline(options=options) as pipeline:
        
        ride_counts = (
            pipeline
            | "Local Cycle Hire Data" >> beam.Create(cycle_hire_data) # this is used to create data locally
            | "Create Pairs" >> beam.Map(lambda x: (x['start_station_id'], x['end_station_id']))
            | "Count Rides" >> beam.combiners.Count.PerElement()
            | "Format Ride Output" >> beam.Map(lambda x: f"{x[0][0]},{x[0][1]},{x[1]}")
            | "Print Ride Counts" >> beam.Map(print)  # print instead of writing to BigQuery
        )


if __name__ == "__main__":
    run_pipeline()