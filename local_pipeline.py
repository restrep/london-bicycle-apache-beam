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


def calculate_station_distance(trip, stations_dict):

    # get ids from the trip
    start_id = trip['start_station_id']
    end_id = trip['end_station_id']

    # Get locations
    start_location = stations_dict.get(start_id, None) # this is a tuple with the location
    end_location =  stations_dict.get(end_id, None) # use method get in case we don't find it get a None


    if start_location and end_location:
        # Get distane
        distance = geodesic(start_location, end_location).kilometers  # Using function from geopy. This was a hint from ML6 from the setup.py
        # We use Yield instead of return so that we get a generator not a full list, that wya it works with apache beam
        return ((start_id, end_id), distance)  


# Define the pipeline
def run_easy_pipeline():
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
    run_easy_pipeline()
    