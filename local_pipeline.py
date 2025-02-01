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
    {'start_station_id': 3, 'end_station_id': 1}
]