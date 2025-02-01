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


def calculate_station_distance(stations, cycle_hire):

    station_locations = {s['id']: (s['latitude'], s['longitude']) for s in stations}  # dictionary that will be use to calclate distance between stations
    
    for trip in cycle_hire:
        start_id = trip['start_station_id'] # get ids from the trip
        end_id = trip['end_station_id']
        
        if start_id not in station_locations or end_id not in station_locations: # Quality check: ignore/consider what happens if there is no start or end
            continue
        
        start_location = station_locations[start_id] # this is a tuple with the location
        end_location = station_locations[end_id] # this is a tuple
        distance = geodesic(start_location, end_location).kilometers  # Using function from geopy. This was a hint from ML6 from the setup.py
        
        yield ((start_id, end_id), distance)  # We use Yield instead of return so that we get a generator not a full list, that wya it works with apache beam


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
            | "Print Ride Counts" >> beam.Map(print)  # print instead of writing to BigQuery or push to GCP
        )


if __name__ == "__main__":
    run_pipeline()

    station_locations = create_station_location_dict(list(stations_data))
    print(stations_data)
    print("#######")
    print(station_locations)
