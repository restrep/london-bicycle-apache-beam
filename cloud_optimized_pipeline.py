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
    project=f"{PROJECT_ID}",  
    job_name="optimized-pipeline",  
    temp_location=f"gs://{BUCKET_NAME}/temp",  
    region="europe-west10", 
    setup_file="./setup.py",  
    requirements_file="./requirements.txt", 
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




def format_easy_output(x):
    return f"{x[0][0]},{x[0][1]},{x[1]}"


def format_hard_output(x):
    return f"{x[0][0]},{x[0][1]},{x[1][0]},{x[1][1]:.2f}"


def run_combined_pipeline():

    def calculate_trip_distance(trip, stations_dict):

        from geopy.distance import geodesic  # only way to solve missing package in gcp

        # get ids from trip
        start_id, end_id = trip
        # get locations
        start_location = stations_dict.get(start_id)
        end_location = stations_dict.get(end_id)

        if start_location and end_location:
            distance = geodesic(start_location, end_location).kilometers
            return distance
        else:
            return 0.0  # critical line depending if want to have these cases in the output or not

    options = project_options

    with beam.Pipeline(options=options) as pipeline:
        stations = (
            pipeline
            | "Read Stations" >> ReadFromBigQuery(query=stations_query, use_standard_sql=True)
            | "Make Stations Dict" >> beam.Map(lambda row: (row["id"], (float(row["latitude"]), float(row["longitude"]))))
        )

        stations_dict = beam.pvalue.AsDict(stations)  # store as side input

        cycle_hire = (
            pipeline
            | "Read Cycle Hires" >> ReadFromBigQuery(query=cycle_hires_query, use_standard_sql=True)
            | "Filter None Station IDs" >> beam.Filter(lambda x: x["start_station_id"] is not None and x["end_station_id"] is not None)
            # this should be done at the query!
        )

        trip_counts = (
            cycle_hire
            | "Get Pairs" >> beam.Map(lambda x: (x["start_station_id"], x["end_station_id"]))
            | "Count Rides" >> beam.combiners.Count.PerElement()
        )

        # Easy test
        easy_output = (
            trip_counts
            | "Format Easy Output" >> beam.Map(format_easy_output)
            | "Write Easy Results" >> beam.io.WriteToText(f"{OUTPUT_PATH}/easy_test", file_name_suffix=".txt", shard_name_template="")
        )

        # Hard test
        hard_output = (
            trip_counts
            | "Calculate distances" >> beam.Map(lambda x, stations: (x[0], (x[1], calculate_trip_distance(x[0], stations))), stations_dict)
            | "Filter Nones" >> beam.Filter(lambda x: x[1][1] is not None)  # a bit redundant but leave it for safety
            | "Multiply by Counts" >> beam.Map(lambda x: (x[0], (x[1][0], x[1][0] * x[1][1])))  #
            | "Format Hard Output" >> beam.Map(format_hard_output)
            | "Write Hard Results" >> beam.io.WriteToText(f"{OUTPUT_PATH}/hard_test", file_name_suffix=".txt", shard_name_template="")
        )


if __name__ == "__main__":
    run_combined_pipeline()
