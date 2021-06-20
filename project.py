# imports libraries
import pandas as pd
from pyspark.sql import SparkSession
import os
from datetime import datetime, timedelta
from pyspark.sql.functions import count, col, isnan, when, udf
from pyspark.sql.types import StructType as ST, StructField as SF, DoubleType as Db, IntegerType as INT,\
StringType as S, DateType as DT, TimestampType as T, LongType as L
import psycopg2
import os
import configparser
from sql_queries import copy_table_queries


config = configparser.ConfigParser()
config.read('dwh.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']

# create a SparkSession
spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()

# Define a schema for different datasets¶

demoSchema = ST([
    SF("city", S()),
    SF("state", S()),
    SF("median_Age", Db()),
    SF("male_population", INT()),
    SF("female_population", INT()),
    SF("total_population", INT()),
    SF("number_of_veterans", INT()),
    SF("foreign_born", INT()),
    SF("average_household_size", Db()),
    SF("state_code", S()),
    SF("race", S()),
    SF("count", INT())
])

modeSchema = ST([
    SF("mode_code", INT()),
    SF("mode_name", S())
    
])

portSchema = ST([
    SF("port_code", S()),
    SF("port_name", S()),
    SF("state_code", S())
])

visaSchema = ST([
    SF("visa_code", INT()),
    SF("visatype", S()),
    SF("category", S()),
    SF("description", S())
])

countrySchema = ST([
   SF("country_code", INT()),
   SF("country", S())
])

# Read in the data from different files

immigration_df =spark.read.load('./sas_data')

df_demograph= spark.read .format("csv")\
.option("header", "true")\
.option("delimiter",";")\
.schema(demoSchema).load("us-cities-demographics.csv")

port_df= spark.read .format("csv")\
.option("header", "true")\
.schema(portSchema).load("port.csv")

visatype_df= spark.read .format("csv")\
.option("header", "true")\
.schema(visaSchema).load("visatype.csv")


mode_of_arrival_df= spark.read .format("csv")\
.option("header", "true")\
.schema(modeSchema).load("mode_of_arrival.csv")

country_df= spark.read .format("csv")\
.option("header", "true")\
.schema(countrySchema).load("countries_and_cities.csv")

def fillNullFunc(df):
    """
    A procedure to clean up the immigration dataframe by filling in the null values of needed columns as follows:
    occup - fill null values with "unknown" (the column has 3088187 null values)
    dtadfile - fill the only null value with "20210618" date of this implementation
    i94bir - fill the null values with "-1" - age cannot be negative (there are 802 null values in the column)
    depdate - fill the null values with "23146"  a future date of this implementation (there are 142457) missing values here)
    i94addr - fill missing values with "unknown" (the column has 152592 missing values)
    i94mode - fill missing values with "4" corresponding to unknown in the visatype table (the column has 239 null values)
    biryear - fill missing values with "2021" (there are 802 missing values here)
    gender - fill missing values with "unknown" (there are 44269 null values in the column)
    INPUT ARG:-> dataframe to be cleaned (immigration_df)
    RETURNS:-> Cleaned Dataframe
    """
    df= df.fillna(value= 23146 , subset = ["depdate"])
    df= df.fillna(value= "20210618" , subset = ["dtadfile"])
    df= df.fillna(value= -1 , subset = ["i94bir"])
    df= df.fillna(value= "unkown" , subset = ["i94addr", "occup", "gender"])
    df= df.fillna(value= 4 , subset = ["i94mode"])
    df= df.fillna(value= 2021 , subset = ["biryear"])
    return df

# Convert the "arrdate" and "depdate" to Date Type

epoch = datetime(1960, 1, 1)
date_converter = udf(lambda x : epoch + timedelta(days = int(x)), DT())

def castColumnFunc(df):
    """
    A function to cast the following columns to the appropriate data types and create respective new columns for them
    INPUT ARG: Dataframe to be transformed
    RETURNS :-> a transformed dataframe
    """
    df = df.withColumn("arr_date", date_converter(immigration_df["arrdate"]))
    df = df.withColumn("dep_date", date_converter(immigration_df["depdate"]))
    df = df.withColumn("age", immigration_df["i94bir"].cast(INT()))
    df = df.withColumn("year_of_birth", immigration_df["biryear"].cast(INT()))
    df = df.withColumn("city_code", immigration_df["i94cit"].cast(INT()))
    df = df.withColumn("country_code", immigration_df["i94res"].cast(INT()))
    df = df.withColumn("month", immigration_df["i94mon"].cast(INT()))
    df = df.withColumn("id", immigration_df["cicid"].cast(INT()))
    df = df.withColumn("visa", immigration_df["i94visa"].cast(INT()))
    df = df.withColumn("year_of_data", immigration_df["i94yr"].cast(INT()))
    df = df.withColumn("mode_code", immigration_df["i94mode"].cast(INT()))
    df = df.withColumn("date_added_to_file", immigration_df["dtadfile"].cast(DT()))
    df = df.withColumn("port_code", immigration_df["i94port"].cast(S()))
    df = df.withColumn("state_code", immigration_df["i94addr"].cast(S()))
    return df

def extractDimFactTables(immigration_df, mode_of_arrival_df, port_df, visatype_df, country_df, df_demogragh):
    """
    A function: -> to extract dimensional and fact tables as follows:
    immigration_fact, date_dim and immigrant_dim tables from immigration_df
    demograph_dim table from df_demograph
    port_dim table from port_df
    visa_dim table from visatype_df
    mode_of_arrival_dim table from mode_of_arrival_df
    country_dim table from country_df
    INPUTS: -> Dataframes(immigration_df, mode_of_arrival_df, port_df, visatype_df, country_df, df_demogragh) to extract from
    RETURNS: -> fact and dimensional tables
    """
    immigration_fact = immigration_df.select(["id", "arr_date", "country_code", "state_code", "port_code", "mode_code", "visatype"])
    date_dim = immigration_df.select(["arr_date", "dep_date", "month", "year_of_data"])
    immigrant_dim = immigration_df.select(["id", "age", "year_of_birth", "gender"])
    mode_of_arrival_dim = mode_of_arrival_df.select(["mode_code","mode_name"])
    port_dim = port_df.select(["port_code", "port_name", "state_code"])
    visa_dim = visatype_df.select(["visa_code", "visatype", "category", "description"])
    country_dim = country_df.select(["country_code", "country"])
    demograph_dim = df_demograph.select(["city", "state", "state_code", "median_age", "male_population", "female_population",
                                    "total_population", "number_of_veterans", "race", "average_household_size", "count"])
    return (immigration_fact, date_dim, immigrant_dim, mode_of_arrival_dim, port_dim, visa_dim, country_dim, demograph_dim)


def write_func(output, list, array_of_paths):
    """
    Procedure : to write or save tables
    INPUT:
        * output - path to save the table
        * list - array of tables
        * array_of_paths - list of unique paths
    RETURNS : NONE
    """
    for table, pathway in zip(list,array_of_paths):
        #table.write.mode("overwrite").csv(os.path.join(output,pathway ))
        file_path = os.path.join(output,pathway )
        table.write.mode("overwrite").csv(file_path)


def load_staging_tables(cur, conn):
    """
    A function:-> to load the tables from AWS S3 into the redshift
    INPUT ARGS: 
        cur:-> cursor to execute database operation
        conn:-> connection to progres database via psycopg2 driver
    RETURNS:-> None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def write_func_in_files(arrayOfTables):
    """
    A function to write or save the extracted tables or dataframes into different files in CSV FORMAT.
    INPUT/ARGUMENT: -> a list of tables specifically, [immigration_df, mode_of_arrival_df, port_df, visatype_df, \
    country_df, df_demograph]
    RETURNS: -> None
    """
    fact, dim1, dim2, dim3, dim4, dim5, dim6, dim7 = extractDimFactTables(*arrayOfTables)
    fact.toPandas().to_csv('immigration_fact.csv')
    dim1.toPandas().to_csv('date_dim.csv')
    dim2.toPandas().to_csv('immigrant_dim.csv')
    dim3.toPandas().to_csv('mode_of_arrival_dim.csv')
    dim4.toPandas().to_csv('port_dim.csv')
    dim5.toPandas().to_csv('visa_dim.csv')
    dim6.toPandas().to_csv('country_dim.csv')
    dim7.toPandas().to_csv('demograph_dim.csv')


def qualityCheckFunc(arrayOfTables, table_names_list):
    """
    A procedure to check the completeness of all the tables
    INPUTS:-
        * arrayOfTables -> A list of the fact and dimensional tables
        * table_names_list -> a list of the names of all the fact and dimensional tables
    RETURNS:
        :-> None
    """
    tables = extractDimFactTables(*arrayOfTables)
    for table, name in zip(tables, table_names_list):
        if table.count() <=0:
            print("The {} table is not complete".format(name))
        else:
            print("The {} table has {} number of rows".format(name, table.count()))


# Run the functions to perform ETL
immigration_df = fillNullFunc(immigration_df)
immigration_df = castColumnFunc(immigration_df)
arrayOfTables = [immigration_df, mode_of_arrival_df, port_df, visatype_df, country_df, df_demograph]
immigration_fact, date_dim, immigrant_dim, mode_of_arrival_dim, port_dim, visa_dim, country_dim, demograph_dim\
        = extractDimFactTables(*arrayOfTables)
list2 = ["immigration_fact", "immigrant_dim", "date_dim", "mode_of_arrival_dim", "port_dim", "visa_dim", "country_dim", "demograph_dim"]

write_func_in_files(arrayOfTables)
qualityCheckFunc(arrayOfTables, list2)
#output = "s3a://data-polake-offorernest/project/"
output = "output/"

write_func(output, arrayOfTables, list2)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    
    conn.close()


if __name__ == "__main__":
    main

