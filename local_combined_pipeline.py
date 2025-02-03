import apache_beam as beam
from geopy.distance import geodesic
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery


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
    {'start_station_id': 3, 'end_station_id': 1},
    {'start_station_id': 3, 'end_station_id': None}
]



def calculate_trip_distance(trip, stations_dict):

    from geopy.distance import geodesic

    # get ids from the trip
    start_id = trip["start_station_id"]
    end_id = trip["end_station_id"]
    # Get locations
    start_location = stations_dict.get(start_id, None)  # this is a tuple with the location
    end_location = stations_dict.get(end_id, None)  # use method get in case we don't find it get a None

    distance = 0.0
    if start_location and end_location:
        distance = geodesic(start_location, end_location).kilometers  # geopy hint from ML6 from the setup.py
    return ((start_id, end_id), (1, distance))  # adding the one as in split pipeline to get both results at onces

def format_easy_output(x):
    return f"{x[0][0]},{x[0][1]},{x[1]}"


def format_hard_output(x):
    return f"{x[0][0]},{x[0][1]},{x[1][0]},{x[1][1]:.2f}"


def run_combined_pipeline():
    options = PipelineOptions(
    runner='DirectRunner'  # this is uset to run locally. On GCP we would use DataFlow
    )

    with beam.Pipeline(options=options) as pipeline:
        stations = (
            pipeline
            | "Read station" >> beam.Create(stations_data)
            | "Make station dict" >> beam.Map(lambda row: (row["id"], (float(row["latitude"]), float(row["longitude"]))))
        )

        stations_dict = beam.pvalue.AsDict(stations)  # store as side input

        cycle_hire = pipeline | "Read cycle hires" >> beam.Create(cycle_hire_data)

        trip_data = (
            cycle_hire
            | "Calculate Trip Data" >> beam.Map(calculate_trip_distance, stations_dict=stations_dict)
            | "Filter Nones" >> beam.Filter(lambda x: x is not None and x[0][0] is not None and x[0][1] is not None)
        )

        # Easy test
        easy_output = (
            trip_data
            | "Get pais" >> beam.Map(lambda x: (x[0][0], x[0][1]))  # Keep only station pairs and ride count
            | "Count Rides" >> beam.combiners.Count.PerElement()
            | "Format Easy Output" >> beam.Map(format_easy_output)
            | "Write Easy Results" >> beam.Map(print) # Print locally
        )

        # Hard test
        hard_output = (
            trip_data
            | "Group by Trip">> beam.CombinePerKey(lambda values: tuple(map(sum, zip(*values))))
            | "Format Hard Output" >> beam.Map(format_hard_output)
            | "Write Hard Results" >> beam.Map(print) # Print locally
        )

if __name__ == "__main__":
    run_combined_pipeline()
