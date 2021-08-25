import logging
import os
from pyspark.sql import SparkSession;
from pyspark.sql.functions import monotonically_increasing_id
from etl import demography_analysis
from etl import immigration_analysis
import configparser
logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-10s %(message)s")
LOG = logging.getLogger("staging to s3")
LOG.setLevel(os.environ.get("LOG_LEVEL", logging.DEBUG))

config = configparser.ConfigParser()

config.read('etl/config.cfg')
class stage_to_s3:
    def model_data():
        spark_session = SparkSession.builder.appName('pipeline').config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.1.2").getOrCreate();

        sc = spark_session.sparkContext
        sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", config['AWS']['KEY'] )
        sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config['AWS']['SECRET'] )
        sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
        sc._jsc.hadoopConfiguration().set("parquet.enable.summary-metadata", "false")

        IMMIGRATION_DATA  = 'datasets/immigration_data/*.parquet';
        immigration_df = spark_session.read.format('parquet').load(IMMIGRATION_DATA, inferSchema=True , header=True);
        CITIES_DATA = 'datasets/us-cities-demographics.csv';
        cities_df = spark_session.read.format('csv').load(CITIES_DATA, sep=";", inferSchema=True , header=True);

        cleaned_immigration_df = immigration_analysis(immigration_df)
        cleaned_cities_data = demography_analysis(cities_df)

        LOG.info(f'========================================= WRITING staging_immigrations_table TABLE TO S3 =========================================')
        cleaned_immigration_df.repartition(1).write.mode('overwrite').parquet(f"s3a://{config['S3']['BUCKET']}/{config['S3']['IMMIGRATION_KEY']}")

        LOG.info(f'========================================= WRITING staging_cities_table TABLE TO S3 =========================================')
        cleaned_cities_data.repartition(1).write.mode('overwrite').parquet(f"s3a://{config['S3']['BUCKET']}/{config['S3']['CITIES_KEY']}")

        return 'Done'