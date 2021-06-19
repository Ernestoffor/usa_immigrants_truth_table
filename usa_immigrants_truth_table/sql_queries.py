import configparser
import os

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES QUERIES
immigration_fact_drop = "DROP TABLE IF EXISTS immigration_fact"
immigrant_dim_drop = "DROP TABLE IF EXISTS immigrant_dim "
date_dim_drop = "DROP TABLE IF EXISTS date_dim"
visa_dim_drop = "DROP TABLE IF EXISTS visa_dim"
mode_of_arrival_dim_drop = "DROP TABLE IF EXISTS mode_of_arrival_dim"
demograph_dim_drop = "DROP TABLE IF EXISTS demograph_dim"
port_dim_drop = "DROP TABLE IF EXISTS port_dim"
country_dim_drop = "DROP TABLE IF EXISTS country_dim"

immigration_fact_create = ("""
        CREATE TABLE IF NOT EXISTS immigration_fact(
        id              INTEGER     PRIMARY KEY,
        arr_date        DATE,
        country_code    INTEGER     ,
        state_code      VARCHAR(5),
        port            VARCHAR(10),
        mode_of_arrival INTEGER    NOT NULL,
        visa            INTEGER,
        visatype        VARCHAR(5)  NOT NULL
        );
""")

date_dim_create = ("""
        CREATE TABLE IF NOT EXISTS date_dim(
        arr_date        DATE,
        dep_date        DATE,
        month           INTEGER,
        year_of_data    INTEGER
        );
""")

immigrant_dim_create = ("""
        CREATE TABLE IF NOT EXISTS immigrant_dim(
        id              INTEGER     PRIMARY KEY,
        age             INTEGER,
        year_of_birth   INTEGER,
        gender          VARCHAR(10)
        );
""")

mode_of_arrival_dim_create = ("""
        CREATE TABLE IF NOT EXISTS mode_of_arrival_dim(
        code            INTEGER PRIMARY KEY,
        mode            VARCHAR(15)
        );
""")

port_dim_create = ("""
    CREATE TABLE IF NOT EXISTS port_dim  (
    code                VARCHAR(20)     PRIMARY KEY,
    port                VARCHAR(50),
    state_code          VARCHAR(15)
    );
""")

visa_dim_create = ("""
    CREATE TABLE IF NOT EXISTS visa_dim(
    visa_code       INTEGER,
    visatype        VARCHAR(15) PRIMARY KEY,
    category        VARCHAR(20),
    description     VARCHAR(128)
    );
""")

country_dim_create = ("""
        CREATE TABLE IF NOT EXISTS country_dim(
        code                INTEGER PRIMARY KEY,
        country             VARCHAR(128)
        );
""")

demograph_dim_create = ("""
        CREATE TABLE IF NOT EXISTS demograph_dim(
        city            VARCHAR(50),
        state           VARCHAR(128),
        state_code      VARCHAR(20),
        median_age      NUMERIC,
        male_population INTEGER,
        female_population INTEGER,
        total_population INTEGER,
        number_of_veterans  INTEGER,
        average_household_size  NUMERIC,
        count               INTEGER
        );
""")


# POPULATE THE TABLES

immigration_fact_copy = ("""
            COPY immigration_fact
            FROM 's3a://data-lake-ernest/project/immigration_fact/part-00000-e34f6ede-3562-4167-8e69-b763ee429f69-c000.csv'
            CREDENTIALS 'aws_iam_role ={}'
            REGION 'us-west-2'
            DELIMITER ','
            REMOVEQUOTES""").format(config.get('IAM','ARN'))

immigrant_dim_copy = ("""
            COPY immigrant_dim
            FROM 's3a://data-lake-ernest/project/immigrant_dim/part-00000-82c1df80-7678-49b7-abf0-44f99e094065-c000.csv'
            CREDENTIALS 'aws_iam_role ={}'
            REGION 'us-west-2'
            DELIMITER ','
            REMOVEQUOTES""").format(config.get('IAM','ARN'))

demograph_dim_copy = ("""
            COPY demograph_dim
            FROM 's3a://data-lake-ernest/project/demograph_dim/part-00000-abd8ea55-2ad5-4036-8526-8cfa7ce8ea07-c000.csv'
            CREDENTIALS 'aws_iam_role ={}'
            REGION 'us-west-2'
            DELIMITER ','
            REMOVEQUOTES""").format(config.get('IAM','ARN'))

visa_dim_copy = ("""
            COPY visa_dim
            FROM 's3a://data-lake-ernest/project/visa_dim/part-00000-a5e3b3c0-1cfb-4f13-9155-f9b50e36bb7f-c000.csv'
            CREDENTIALS 'aws_iam_role ={}'
            REGION 'us-west-2'
            DELIMITER ','
            REMOVEQUOTES""").format(config.get('IAM','ARN'))

country_dim_copy = ("""
            COPY country_dim
            FROM 's3a://data-lake-ernest/project/country_dim/part-00000-d73fcc54-75e4-4c8f-8b92-cee6ea907d0b-c000.csv'
            CREDENTIALS 'aws_iam_role ={}'
            REGION 'us-west-2'
            DELIMITER ','
            REMOVEQUOTES""").format(config.get('IAM','ARN'))

port_dim_copy = ("""
            COPY port_dim
            FROM 's3a://data-lake-ernest/project/port_dim/part-00000-976a55fb-324c-42c7-8d47-17f03971fcff-c000.csv'
            CREDENTIALS 'aws_iam_role ={}'
            REGION 'us-west-2'
            DELIMITER ','
            REMOVEQUOTES""").format(config.get('IAM','ARN'))

mode_of_arrival_dim_copy = ("""
            COPY mode_of_arrival_dim
            FROM 's3a://data-lake-ernest/project/mode_of_arrival_dim/part-00000-5f4861ae-1157-4f67-b74b-543d4d437e4e-c000.csv'
            CREDENTIALS 'aws_iam_role ={}'
            REGION 'us-west-2'
            DELIMITER ','
            REMOVEQUOTES""").format(config.get('IAM','ARN'))
date_dim_copy = ("""
            COPY date_dim
            FROM 's3a://data-lake-ernest/project/date_dim/part-00000-4d15bc0e-9d1d-4904-b5c5-c48dfbbe92c0-c000.csv'
            CREDENTIALS 'aws_iam_role ={}'
            REGION 'us-west-2'
            DELIMITER ','
            REMOVEQUOTES""").format(config.get('IAM','ARN'))


# LIST OF QUERIES

create_tables_list = [immigration_fact_create, immigrant_dim_create, visa_dim_create, mode_of_arrival_dim_create,
                port_dim_create, country_dim_create, demograph_dim_create, date_dim_create]

copy_table_queries = [immigration_fact_copy, immigrant_dim_copy, visa_dim_copy, mode_of_arrival_dim_copy,
                port_dim_copy, country_dim_copy, demograph_dim_copy, date_dim_copy]


drop_table_queries = [immigration_fact_drop, immigrant_dim_drop, date_dim_drop, visa_dim_drop, mode_of_arrival_dim_drop,
                        demograph_dim_drop, country_dim_drop, port_dim_drop]
