#	Introduction          
The government want to analyse the influx of immigrant in USA and determined the percentage of immigratant per state. 
Some industrial standard tools and assumption were considered in this project, such as modelling stage, storage, staging and scheduling. The Data Engineering tools like pandas, Amazon Redshift, Spark, and Airflow will be used for exploration, storage and manupilation of data. 

 The goal of this project is to ensure the right data is gathered for a successful analysis by the team. 

### Data description
* I94 Immigration Data
This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. This is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project. https://www.trade.gov/national-travel-and-tourism-office

* U.S. City Demographic Data
This data comes from OpenSoft. It is an opensource data which can be accessed either by manual downloading or api. https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/


### The data set structure.
STAGING_IMMIGRATIONS (staging table) as loaded from S3
|-- immigrant_id: (bigint) A unique, spark-generated id representing each immigrant
|-- year: (integer) year the data was captured
|-- month: (integer) month the data was captured
|-- resident_country_code: (integer) The resident country code of the immigrant.
|-- arrival_date: (date) Date of immigrants arrival
|-- address: (varchar) Current US address of the immigrant (state code)
|-- departure_date: (date) Date of immigrants departure (if available)
|-- age: (integer) age of immigrant (as at when this data was captured)
|-- visa_code: (integer) specifies the type of visa used. Equivalent to Business, Pleasure or Student.
|-- birth_year: (integer) Immigrants year of birth
|-- gender: (varchar) Immigrants gender
|-- airline: (varchar) The airline used by immigrant
|-- mode: (varchar) mode of transport (code) equivalent to LAND, SEA, AIR. If available.
|-- resident_country: (varchar) The resident country of the immigrant
|-- visa_type: (varchar) specifies the type of visa used
|-- state_address: (varchar) current US state address (unabbreviated).
|-- transport_mode: (varchar) mode of transport (code derived).

STAGING_DEMOGRAPHY (staging table) as loaded from S3
|-- city_id: (bigint) unique, spark generated city identifier
|-- city: (varchar) US city name
|-- state: (varchar) US state name
|-- median_age: (real) median age of residents in this city
|-- male_population: (integer) male population of residents in the corresponding city
|-- female_population: (integer) female population of residents in the corresponding city
|-- total_population: (integer) total population of residents in the corresponding city
|-- num_of_veterans: (integer) total number of veterans in each corresponding city
|-- no_of_immigrants: (integer) total number of immigrants in each corresponding city
|-- avg_household_size: (real) average household size of city residents
|-- state_code: (varchar) US state code
|-- race: (varchar) most dominant rance in each city

US_GEOGRAPHY (fact table)
|-- city_id: (bigint) unique, spark generated city identifier
|-- male_population: (integer) male population of residents in the corresponding city
|-- female_population: (integer) female population of residents in the corresponding city
|-- total_population: (integer) total population of residents in the corresponding city
|-- num_of_veterans: (integer) total number of veterans in each corresponding city
|-- no_of_immigrants: (integer) total number of immigrants in each corresponding city
|-- avg_household_size: (real) average household size of city residents

IMMIGRANTS_FACTS (fact table)
|-- immigrant_id: (bigint) A unique, spark-generated id representing each immigrant
|-- year: (integer) year the data was captured
|-- month: (integer) month the data was captured
|-- visa_code: (integer) specifies the type of visa used. Equivalent to Business, Pleasure or Student.
|-- mode: (varchar) mode of transport (code) equivalent to LAND, SEA, AIR. If available.

US_CITIES (dim table)
|-- city_id: (bigint) unique, spark generated city identifier
|-- city: (varchar) US city name
|-- state_code: (varchar) US state code

US_STATES (dim table)
|-- state: (varchar) US state name
|-- state_code: (varchar) US state code

VISA_TYPES (dim table)
|-- visa_code: (integer) specifies the type of visa used. Equivalent to Business, Pleasure or Student.
|-- visa_type: (varchar) specifies the type of visa used

TRANSPORT_MODES (dim table)
|-- mode: (varchar) mode of transport (code) equivalent to LAND, SEA, AIR. If available.
|-- transport_mode: (varchar) mode of transport (code derived).

TRAVEL_INFO (dim table)
|-- immigrant_id: (bigint) A unique, spark-generated id representing each immigrant
|-- arrival_date: (date) Date of immigrants arrival
|-- departure_date: (date) Date of immigrants departure (if available)
|-- airline: (varchar) The airline used by immigrant

IMMIGRANTS (dim table)
|-- immigrant_id: (bigint) A unique, spark-generated id representing each immigrant
|-- age: (integer) age of immigrant (as at when this data was captured)
|-- birth_year: (integer) Immigrants year of birth
|-- gender: (varchar) Immigrants gender
|-- resident_country: (varchar) The resident country of the immigrant
|-- address: (varchar) Current US address of the immigrant (state code)


### Set up Environment
*   Install or Update your python
*	Install or update Airflow 
*	Install or update docker and docker-compose
*   Set up AWS Redshift datawarehose cluster

###  Functionality
#### 1 	Stage data from local to s3
#### 2   Create Database
#### 3   Drop table if exits
#### 4   Create table if not exists
#### 5   Build ETL Processes
#### 6   Insert into table
<img width="1436" alt="Screenshot 2021-08-26 at 00 01 25" src="https://user-images.githubusercontent.com/50584494/130870324-04c676cf-2b45-42d4-9630-3ae1419e9b70.png">

### What is the use of each file
*   dag folder: This contain the dag python script that setup the dag tasks.
*   plugin folder: The folder contains the custom operators, sql helper query and python pyspark for transforming the data. 
*   docker and db-data for environment and airflow setup.


### How to run the scripts
*   Build the Docker container image using the following command: `./mwaa-local-env build-image`.
*	Runs a local Apache Airflow environment configuration: `./mwaa-local-env start`.
*	Accessing the Airflow UI: `http://localhost:8080/`

### NOTE: By default, the bootstrap.sh script creates a username and password for your local Airflow environment.

*	Username: `admin`
*	Password: `test`

<img width="1432" alt="Screenshot 2021-08-26 at 00 02 30" src="https://user-images.githubusercontent.com/50584494/130870615-498b6f8f-3929-4459-9b60-8910a6bd5621.png">

### Additional Requirements

Qustion 1: If the data was increased 100 times:

Solution: The Spark used in this project would still be the best possible tools to be used for the Data Exploration and Manipulation processes. s3 is cheap and scalable, therefore, staging the data in s3 has been the best option.

Qustion 2: If the pipeline was to be run on a daily basis by 7am.
Solution: The Airflow considered in this project as pipeline management already take care of the schedling. In defining the DAG, several parameters are provided by airflow for the data engineer to set-up to make scheduling seemless. To achieve this daily 7am schedule, below is an example of what the DAG arguments would look like:

    dag = DAG(
        dag_id="scheduling_dag_id",
        start_date=(2021,07,15,07,00,00)
        schedule_interval="@daily"
    )

Qustion 3: If the database needs to be accessed by 100+ people

Solution: The datawarehouse used in this project automatically creates snapshots that tracks changes to the cluster. The Amazon's Redshift would do the storage work. Redshift is a data warehouse cloud-based system, storing data on a peta-byte scale. Replication would be advisable especially in cases where the database users are in several geographical locations.



<img width="1433" alt="Screenshot 2021-08-25 at 23 59 38" src="https://user-images.githubusercontent.com/50584494/130870148-86ca666f-5b5e-46fb-8746-d51bc4260109.png">





