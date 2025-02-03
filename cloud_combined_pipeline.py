import apache_beam as beam
from geopy.distance import geodesic
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

# Define GCS bucket
PROJECT_ID = "london-bicycle-apache-beam"
BUCKET_NAME = "ml6-test-london-bikes"
OUTPUT_PATH = f"gs://{BUCKET_NAME}/output"

project_options = PipelineOptions(
    runner="DataflowRunner",
    project=f"{PROJECT_ID}",  # Replace with your GCP project ID
    job_name="combined-pipeline",  # Give your job a descriptive name
    temp_location=f"gs://{BUCKET_NAME}/temp",  # Replace with your GCS bucket
    region="europe-west10",  # Choose appropriate region
    setup_file="./setup.py",  # Required for installing dependencies
    requirements_file="./requirements.txt",  # Specify Python package dependencies
)


stations_query = """
                    SELECT 
                        id,
                        latitude,
                        longitude
                    FROM `bigquery-public-data.london_bicycles.cycle_stations`
                """

cycle_hires_query = """
                    SELECT 
                        start_station_id,
                        end_station_id
                    FROM `bigquery-public-data.london_bicycles.cycle_hire`
                """  # WHERE start_station_id IS NOT NULL to make things faster


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
    options = project_options

    with beam.Pipeline(options=options) as pipeline:
        stations = (
            pipeline
            | "Read Stations" >> ReadFromBigQuery(query=stations_query, use_standard_sql=True)
            | "Make Stations Dict" >> beam.Map(lambda row: (row["id"], (float(row["latitude"]), float(row["longitude"]))))
        )

        stations_dict = beam.pvalue.AsDict(stations)  # store as side input

        cycle_hire = pipeline | "Read Cycle Hires" >> ReadFromBigQuery(query=cycle_hires_query, use_standard_sql=True)

        trip_data = cycle_hire | "Calculate Trip Data" >> beam.Map(calculate_trip_distance, stations_dict=stations_dict)

        # Easy test
        easy_output = (
            trip_data
            | "Get Pairs" >> beam.Map(lambda x: (x[0][0], x[0][1]))  # Keep only station pairs and ride count
            | "Count Rides" >> beam.combiners.Count.PerElement()
            | "Format Easy Output" >> beam.Map(format_easy_output)
            | "Write Easy Results" >> beam.io.WriteToText(f"{OUTPUT_PATH}/easy_test", file_name_suffix=".txt", shard_name_template="")
        )

        # Hard test
        hard_output = (
            trip_data
            | "Group by Trip" >> beam.CombinePerKey(lambda values: tuple(map(sum, zip(*values))))
            | "Format Hard Output" >> beam.Map(format_hard_output)
            | "Write Hard Results" >> beam.io.WriteToText(f"{OUTPUT_PATH}/hard_test", file_name_suffix=".txt", shard_name_template="")
        )


if __name__ == "__main__":
    run_combined_pipeline()
    