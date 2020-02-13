"""
lrkspark_job.py
~~~~~~~~~~

This Python module contains an example Apache Spark job definition
that implements best practices for production jobs. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/spark_config.json \
    jobs/lrkspark_job.py

where packages.zip contains Python modules required by Spark job (in
this example it contains a class to provide access to Spark's logger),
which need to be made available to each executor process on every node
in the cluster; spark_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the spark job; and, spark_job.py contains the Spark application
to be executed by a driver process on the Spark master node.

For more details on submitting Spark applications, please see here:
http://spark.apache.org/docs/latest/submitting-applications.html

"""

from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit

from dependencies.spark import start_spark


def main():
    """Main script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='my_lrkspark_job',
        files=['configs/spark_config.json'])

    # log that main spark job is starting
    log.warn('lrkspark_job is up-and-running')

    # execute  pipeline
    data = extract_data(spark)
    load_data(data_transformed)
    data_transformed = transform_data(data, config['Max_Temp_'])
    load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_lrkspark_job is finished')
    spark.stop()
    return None


def extract_data(spark):
    """Load data from CSV file format.

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = (
        spark
        .read
        .csv('/Users/LRK/project-folder/GreenFlag/DataEngineerTest/weather.20160201.csv', header=True))

    return df

def load_data(df):
    """Collect data locally and write to PARQUET.

    :param df: DataFrame to print.
    :return: None
    """
    (df
     .coalesce(1)
     .write
     .parquet('/Users/LRK/project-folder/GreenFlag/DataEngineerTest/input201-parquet', mode='overwrite'))

def read_data(df):
    """Collect data from parqet file.

    :param df: read from parquet.
    :return: None
    """
    (df
     .sqlContext
     .read
     .format('parquet')
     .load('/Users/LRK/project-folder/GreenFlag/DataEngineerTest/ \
        input201-parquet'))


def casting_datatypes(df):
    """Casting to correct data types.

    :param df: DataFrame to print.
    :return: None
    """
    df = df.select(
        df.ForecastSiteCode.cast('int'),
        df.ObservationTime.cast('timestamp'),
        df.ObservationDate.cast('date'),
        df.WindDirection.cast('int'),
        df.WindSpeed.cast('int'),
        df.WindGust.cast('int'),
        df.Visibility.cast('int'),
        df.ScreenTemperature.cast('int'),
        df.Pressure.cast('int'),
        df.SignificantWeatherCode.cast('int'),
        df.SiteName.cast('int'),
        df.Latitude.cast('float'),
        df.Longitude.cast('float'),
        df.Region.cast('string'),
        df.Country.cast('string'))
    
def crate_temp_view(df):
    """Creating temp view.

    :param df: Temp_view.
    :return: None
    """
    (df.createOrReplaceTempView("TEMP_DF"))



def transform_data(TEMP_DF, Max_Temp_):
    """Transform original dataset.

    :param df: Input DataFrame.
    :param max_temp_: max temp, day and region
    :return: Transformed DataFrame.
    """

    df_transformed = (
        spark
        .sql("select ObservationDate,ScreenTemperature,Region from \
            TEMP_DF where ScreenTemperature=(select max(ScreenTemperature)") \
        .collect()[0])

    return df_transformed

def loadresult_data(df):
    """Collect data and write to parquet.

    :param df: DataFrame 
    :return: None
    """
    (df_transformed
     .coalesce(1)
     .write
     .parquet('/Users/LRK/project-folder/GreenFlag/DataEngineerTest/ \
        result', mode='overwrite'))
    
    return None

def create_test_data(spark, config):
    """Create test data.

    This function creates both both pre- and post- transformation data
    saved as Parquet files in tests/test_data. This will be used for
    unit tests as well as to load as part of the example spark job.
    :return: None
    """
    # create example data from scratch
   
    df = spark.read.csv('/Users/LRK/project-folder/GreenFlag/weathertest.csv', header=True)

    # write to Parquet file format
    (df.coalesce(1).write
     .parquet('/Users/LRK/project-folder/GreenFlag/sparkjob/test_data/test', mode='overwrite'))

    # create transformed version of data
    df_tf = transform_data(df, config['Max_Temp_'])

    # write transformed version of data to Parquet
    (df_tf
     .coalesce(1)
     .write
     .parquet('/Users/LRK/project-folder/GreenFlag/DataEngineerTest/test_data/test_result', mode='overwrite'))

    return None


# entry point for PySpark application
if __name__ == '__main__':
    main()
