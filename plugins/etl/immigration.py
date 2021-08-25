import logging
import os
from pyspark.sql.types import  StringType, IntegerType, DateType
from pyspark.sql.functions import monotonically_increasing_id, to_date
import pandas as pd
import configparser
from pyspark.sql.functions import udf
logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-10s %(message)s")
LOG = logging.getLogger("DataQualityOperator")
LOG.setLevel(os.environ.get("LOG_LEVEL", logging.DEBUG))
config = configparser.ConfigParser()
config.read('config.cfg')

new_column_names = {
	    'i94yr': 'year',
	    'i94mon': 'month',
	    'i94res': 'resident_country_code',
	    'arrdate': 'arrival_date',
	    'i94mode': 'mode',
	    'i94addr': 'address',
	    'depdate': 'departure_date',
	    'i94bir': 'age',
	    'i94visa': 'visa_code',
	    'biryear': 'birth_year'
	}
class immigration_analysis:

	@udf
	def get_country(self, code):
	    if code is None or code == '':
	        return 'not provided'
	    if code not in config['RESIDENTIAL_COUNTRY']:
	        return f'invalid code {code}'
	    return config['RESIDENTIAL_COUNTRY'][code]

	@udf
	def get_date(self, date):
	    if date is None:
	        return 'not provided'
	    if date == 'not provided':
	        return 'not provided'
	    formatted_datetime =  pd.to_datetime(pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1'))
	    return str(formatted_datetime.date())

	@udf
	def get_visa_type(self, visa_code):
	    if visa_code is None or visa_code == '':
	        return 'not provided'
	    if visa_code == 'not provided':
	        return 'not provided'
	    return config['VISA_TYPES'][visa_code];

	@udf
	def get_address(self, abbr):
	    if (abbr == 'not provided') or abbr is None:
	        return 'not provided'
	    if abbr not in config['US_STATES']:
	        return f'invalid code {abbr}'
	    return config['US_STATES'][abbr]

	@udf
	def get_mode(self, code):
	    if  code is None or code == "" or code == "null":
	        return 4
	    return int(code)

	@udf
	def get_transport_mode(self, code):
	    if code == 4 or code is None:
	        return 'not provided'
	    if str(code) not in config['TRANSPORT_MODE']:
	        return f'invalid code {code}'
	    return config['TRANSPORT_MODE'][str(code)]

	def remane_columns(self, df, columns_dictionary):
	    new_df = df
	    for key, value in columns_dictionary.items():
	        new_df = new_df.withColumnRenamed(key, value);
	    return new_df


	def clean_immigration_data(
	  self,   dataframe
	):

	    LOG.info('========================================= CLEANING IMMIGRATION DATA =========================================')

	    new_df = self.remane_columns(dataframe, new_column_names)

	    new_df = new_df.select(
	        new_df.year.cast(IntegerType()), 
	        new_df.month.cast(IntegerType()), 
	        new_df.resident_country_code.cast(IntegerType()),
	        new_df.arrival_date, 
	        new_df.address,
	        new_df.departure_date,
	        new_df.age.cast(IntegerType()),
	        new_df.visa_code.cast(IntegerType()),
	        new_df.birth_year.cast(IntegerType()),
	        new_df.gender, 
	        new_df.airline,
	        new_df.mode.cast(IntegerType())
	    ).na.fill(value='not provided').distinct()

	    # get country name for residential code
	    new_df = new_df.withColumn(
	        'resident_country', 
	        self.get_country(
	            new_df.resident_country_code.cast(StringType()))
	        ).withColumn(
	            'arrival_date',
	            self.to_date(self.get_date(new_df.arrival_date))
	    ).withColumn(
	        'departure_date',
	        self.get_date(new_df.departure_date)
	    ).withColumn(
	        'visa_type',
	        self.get_visa_type(new_df.visa_code.cast(StringType()))
	    ).withColumn(
	        'state_address',
	        self.get_address(new_df.address)
	    ).withColumn(
	        'mode',
	        self.get_mode(new_df.mode)
	    )
	    new_df = new_df.withColumn(
	        'transport_mode',
	        self.get_transport_mode(new_df.mode)
	    ).withColumn('immigrant_id', monotonically_increasing_id())
	    new_df = new_df.withColumn(
	    'departure_date',
	    new_df.departure_date.cast(DateType())).withColumn(
	    'arrival_date',
	    new_df.arrival_date.cast(DateType()))
	    return new_df