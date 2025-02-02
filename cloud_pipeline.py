import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
from geopy.distance import geodesic


# Define GCS bucket
PROJECT_ID = "london-bicycle-apache-beam"
BUCKET_NAME = "ml6-test-london-bikes"
OUTPUT_PATH = f'gs://{BUCKET_NAME}/output'

project_options = PipelineOptions(
        runner='DataflowRunner',
        project=f"{PROJECT_ID}",  # Replace with your GCP project ID
        job_name='london-bikes-ml6',  # Give your job a descriptive name
        temp_location=f'gs://{BUCKET_NAME}/temp',  # Replace with your GCS bucket
        region='europe-west10',  # Choose appropriate region
        setup_file='./setup.py',  # Required for installing dependencies
        requirements_file='requirements.txt'  # Specify Python package dependencies
        region="europe-west10"
    )


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
    options = project_options
    
    with beam.Pipeline(options=options) as pipeline:

        cycle_hires = (
            pipeline 
            | 'Read cycle hires' >> ReadFromBigQuery(
                query="""
                    SELECT 
                        start_station_id,
                        end_station_id
                    FROM `bigquery-public-data.london_bicycles.cycle_hire`
                """,
                use_standard_sql=True
            )
        )
        
        ride_counts = (
            cycle_hires
            | "Create Pairs" >> beam.Map(lambda x: (x['start_station_id'], x['end_station_id']))
            | "Count Rides" >> beam.combiners.Count.PerElement()
            | "Format Ride Output" >> beam.Map(lambda x: f"{x[0][0]},{x[0][1]},{x[1]}")
            | 'WriteResults' >> beam.io.WriteToText(f"{OUTPUT_PATH}/easy_test", file_name_suffix='.txt', shard_name_template='')
            #| "Print Ride Counts" >> beam.Map(print)  # print instead of writing to BigQuery or push to GCP
        )

def run_hard_pipeline():
    options = project_options
    
    with beam.Pipeline(options=options) as pipeline:

        stations = (
            pipeline
            | 'Read station' >> ReadFromBigQuery(
                query="""
                    SELECT 
                        id,
                        latitude,
                        longitude
                    FROM `bigquery-public-data.london_bicycles.cycle_stations`
                """,
                use_standard_sql=True
            )
            | 'Make station dict' >> beam.Map(
                lambda row: (row['id'], (float(row['latitude']), float(row['longitude'])))
            ) )
        stations_dict = beam.pvalue.AsDict(stations)


        cycle_hires = (
            pipeline 
            | 'Read cycle hires' >> ReadFromBigQuery(
                query="""
                    SELECT 
                        start_station_id,
                        end_station_id
                    FROM `bigquery-public-data.london_bicycles.cycle_hire`
                """,
                use_standard_sql=True
            )
        )

        # Process and aggregate distances
        station_distances = (
            cycle_hires
            | "Calculate Distances" >> beam.Map(calculate_station_distance, stations_dict=stations_dict)
            | "Sum Distances" >> beam.CombinePerKey(sum)
            | "Format Distance Output" >> beam.Map(lambda x: f"{x[0][0]},{x[0][1]},{x[1]:.2f}")
            | "Print Distasnces" >> beam.Map(print)  # Print instead of writing to BigQuery
       )


if __name__ == "__main__":
    run_easy_pipeline()
    #run_hard_pipeline()
    