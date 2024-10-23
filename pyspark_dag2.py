from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.operators.python import  BranchPythonOperator
#from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import os
os.environ['PYSPARK_PYTHON']=='/home/maher/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] ='/home/maher/anaconda3/bin/python'
#os.environ['JAVA_HOME'] = '/opt/java'
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
#import findspark
#findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql.functions import lit, create_map, col, when, lower, last
import csv
import sys
default_args = {
    'owner': 'Maher',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
spark = SparkSession.builder \
        .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:9.4.0.jre8") \
        .getOrCreate()
API_URL='https://datacatalogapi.worldbank.org/dexapps/fone/api/apiservice?datasetId=DS00975&resourceId=RS00905&type=json'
limit=50000
rows = 1342181
def initialize_offset():
    initial_offset = 600000
    Variable.set("api_offset", initial_offset)
    
"""
def create_spark_session():
    global spark
    spark = SparkSession.builder \
        .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:9.4.0.jre8") \
        .getOrCreate()
    return
"""
def fetch_data_from_api(**kwargs):
    offset = Variable.get("api_offset", default_var=600000)
    limit = 50000
    total_rows = rows
    
    params = {
        "top": limit,
        "skip": offset
    }
    
    response = requests.get(API_URL, params=params)
    if response.status_code == 200:
        data = response.json().get('data')
        if data:
            # Store the fetched data in XCom
            kwargs['ti'].xcom_push(key='api_data', value=data)
            
            # Increment the offset
            new_offset = int(offset) + limit
            if new_offset < total_rows:
                Variable.set("api_offset", new_offset)
            else:
                Variable.set("api_offset", total_rows)  # Stop at total rows
            
    else:
        raise Exception("Failed to fetch data from API.")

def filter_and_drop_columns(**kwargs):
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import lower
    global spark
    schema=StructType([StructField('end_of_period', StringType(), True), 
                    StructField('loan_number', StringType(), True), 
                    StructField('region', StringType(), True), 
                    StructField('country_code', StringType(), True), 
                    StructField('country', StringType(), True), 
                    StructField('borrower', StringType(), True), 
                    StructField('guarantor_country_code', StringType(), True), 
                    StructField('guarantor', StringType(), True), 
                    StructField('loan_type', StringType(), True), 
                    StructField('loan_status', StringType(), True), 
                    StructField('interest_rate', DoubleType(), True), 
                    StructField('currency_of_commitment', StringType(), True),
                    StructField('project_id', StringType(), True), 
                    StructField('project_name_', StringType(), True), 
                    StructField('original_principal_amount', DoubleType(), True), 
                    StructField('cancelled_amount', DoubleType(), True), 
                    StructField('undisbursed_amount', DoubleType(), True), 
                    StructField('disbursed_amount', DoubleType(), True), 
                    StructField('repaid_to_ibrd', DoubleType(), True), 
                    StructField('due_to_ibrd', DoubleType(), True), 
                    StructField('exchange_adjustment', DoubleType(), True), 
                    StructField('borrowers_obligation', DoubleType(), True), 
                    StructField('sold_3rd_party', DoubleType(), True), 
                    StructField('repaid_3rd_party', DoubleType(), True), 
                    StructField('due_3rd_party', DoubleType(), True), 
                    StructField('loans_held', DoubleType(), True), 
                    StructField('first_repayment_date', StringType(), True), 
                    StructField('last_repayment_date', StringType(), True), 
                    StructField('agreement_signing_date', StringType(), True), 
                    StructField('board_approval_date', StringType(), True), 
                    StructField('effective_date_most_recent', StringType(), True), 
                    StructField('closed_date_most_recent', StringType(), True), 
                    StructField('last_disbursement_date', StringType(), True)])
    data = kwargs['ti'].xcom_pull(key='api_data')
    df = spark.createDataFrame(data, schema=schema)
    
    dates=["30-Jun-2024","30-Jun-2023","30-Jun-2022","30-Jun-2021","30-Jun-2020","30-Jun-2019","30-Jun-2018","30-Jun-2017","30-Jun-2016","30-Jun-2015","30-Jun-2014"
       ,'30-Jun-2013',"30-Jun-2012","30-Jun-2011"]

    columns_to_drop=['currency_of_commitment','exchange_adjustment','last_disbursement_date','agreement_signing_date','effective_date_most_recent','closed_date_most_recent']

    
    df_filtered = df.filter(df.end_of_period.isin(dates))
    df_dropped = df_filtered.drop(*columns_to_drop)
    
    # Convert string columns to lowercase
    string_columns = [field.name for field in df_dropped.schema.fields if isinstance(field.dataType, StringType)]
    for col_name in string_columns:
        df_dropped = df_dropped.withColumn(col_name, lower(df_dropped[col_name]))
    
    rows = df_dropped.collect()
    serialized_data = {"columns": df_dropped.columns, "data": [row.asDict() for row in rows]}
    kwargs['ti'].xcom_push(key='filtered_data', value=serialized_data)


def read_csv_cleaning_files():
    import csv

# Open the CSV file
    with open('/home/maher/airflow/dags/Status_Cleaning.csv', mode='r', newline='', encoding='utf-8') as file:
        # Create a DictReader object
        csv_reader = csv.reader(file)
        status = {}
        next(csv_reader)
        for row in csv_reader:
            key = row[0].lower()   # Assuming the first column is the key
            value = row[1].lower() # Assuming the second column is the value
            status[key] = value
    
# Print the result
    ##return status

    with open('/home/maher/airflow/dags/loan_status_BK.csv', mode='r', newline='', encoding='utf-8') as file:
        # Create a DictReader object
        csv_reader = csv.reader(file)
        loan_status_bk = {}
        next(csv_reader)
        for row in csv_reader:
            key = row[0].lower()   # Assuming the first column is the key
            value = row[1].lower() # Assuming the second column is the value
            loan_status_bk[key] = value
    


    with open('/home/maher/airflow/dags/Type_Cleaning.csv', mode='r', newline='', encoding='utf-8') as file:
        # Create a DictReader object
        csv_reader = csv.reader(file)
        type_dict = {}
        next(csv_reader)
        for row in csv_reader:
            key = row[0].lower()   # Assuming the first column is the key
            value = row[1].lower() # Assuming the second column is the value
            type_dict[key] = value
    

    

    with open('/home/maher/airflow/dags/loan_type_BK.csv', mode='r', newline='', encoding='utf-8') as file:
        # Create a DictReader object
        csv_reader = csv.reader(file)
        loan_type_bk = {}
        next(csv_reader)
        for row in csv_reader:
            key = row[0].lower()   # Assuming the first column is the key
            value = row[1].lower() # Assuming the second column is the value
            loan_type_bk[key] = value
    



    with open("/home/maher/airflow/dags/Countries_Cleaning.csv", mode='r', newline='', encoding='utf-8') as file:
        # Create a DictReader object
        csv_reader = csv.reader(file)
        countries = {}
        next(csv_reader)
        for row in csv_reader:
            key = row[0].lower()   # Assuming the first column is the key
            value = row[1].lower() # Assuming the second column is the value
            countries[key] = value

# Print the result
    

    with open('/home/maher/airflow/dags/country_BK.csv', mode='r', newline='', encoding='utf-8') as file:
    # Create a DictReader object
        csv_reader = csv.reader(file)
        country_bk = {}
        next(csv_reader)
        for row in csv_reader:
            key = row[0].lower()   # Assuming the first column is the key
            value = row[1].lower() # Assuming the second column is the value
            country_bk[key] = value
    


    with open('/home/maher/airflow/dags/Regions_Cleaning.csv', mode='r', newline='', encoding='utf-8') as file:
        # Create a DictReader object
        csv_reader = csv.reader(file)
        regions = {}
        next(csv_reader)
        for row in csv_reader:
            key = row[0].lower()   # Assuming the first column is the key
            value = row[1].lower() # Assuming the second column is the value
            regions[key] = value
   

    with open('/home/maher/airflow/dags/regions_BK.csv', mode='r', newline='', encoding='utf-8') as file:
        # Create a DictReader object
        csv_reader = csv.reader(file)
        regions_Bk = {}
        next(csv_reader)
        for row in csv_reader:
            key = row[0].lower()   # Assuming the first column is the key
            value = row[1].lower() # Assuming the second column is the value
            regions_Bk[key] = value
    



    with open('/home/maher/airflow/dags/Borrower_cleaning.csv', mode='r', newline='', encoding='utf-8') as file:
        # Create a DictReader object
        csv_reader = csv.reader(file)
        borrower_dict = {}
        next(csv_reader)
        for row in csv_reader:
            key = row[0].lower()   # Assuming the first column is the key
            value = row[1].lower() # Assuming the second column is the value
            borrower_dict[key] = value
    

    with open('/home/maher/airflow/dags/borrower_BK_updated.csv', mode='r', newline='', encoding='utf-8') as file:
        # Create a DictReader object
        csv_reader = csv.reader(file)
        borrower_bk = {}
        next(csv_reader)
        for row in csv_reader:
            key = row[0].lower()   # Assuming the first column is the key
            value = row[1].lower() # Assuming the second column is the value
            borrower_bk[key] = value
    dict={"status":status,"loan_status_bk":loan_status_bk,"type_dict":type_dict,"loan_type_bk":loan_type_bk,"countries":countries,"country_bk":country_bk,"regions":regions,"regions_Bk":regions_Bk,"borrower_dict":borrower_dict,"borrower_bk":borrower_bk}
    return(dict)

def clean_data(diction,**kwargs):
    from pyspark.sql.functions import create_map, lit
    global spark
    schema=StructType([StructField('end_of_period', StringType(), True), 
                    StructField('loan_number', StringType(), True), 
                    StructField('region', StringType(), True), 
                    StructField('country_code', StringType(), True), 
                    StructField('country', StringType(), True), 
                    StructField('borrower', StringType(), True), 
                    StructField('guarantor_country_code', StringType(), True), 
                    StructField('guarantor', StringType(), True), 
                    StructField('loan_type', StringType(), True), 
                    StructField('loan_status', StringType(), True), 
                    StructField('interest_rate', DoubleType(), True),
                    StructField('project_id', StringType(), True), 
                    StructField('project_name_', StringType(), True), 
                    StructField('original_principal_amount', DoubleType(), True), 
                    StructField('cancelled_amount', DoubleType(), True), 
                    StructField('undisbursed_amount', DoubleType(), True), 
                    StructField('disbursed_amount', DoubleType(), True), 
                    StructField('repaid_to_ibrd', DoubleType(), True), 
                    StructField('due_to_ibrd', DoubleType(), True),  
                    StructField('borrowers_obligation', DoubleType(), True), 
                    StructField('sold_3rd_party', DoubleType(), True), 
                    StructField('repaid_3rd_party', DoubleType(), True), 
                    StructField('due_3rd_party', DoubleType(), True), 
                    StructField('loans_held', DoubleType(), True), 
                    StructField('first_repayment_date', StringType(), True), 
                    StructField('last_repayment_date', StringType(), True), 
                    StructField('board_approval_date', StringType(), True)])
    # Get filtered data from XCom
    filtered_data = kwargs['ti'].xcom_pull(key='filtered_data')
    columns=filtered_data['columns']
    data=filtered_data['data']
    df = spark.createDataFrame(data,schema=schema)
    
    # Example of cleaning
    dict = read_csv_cleaning_files()
    df_cleaned = df.replace(diction['status'], subset=['loan_status']).replace(diction['type_dict'],subset=['loan_type'])\
        .replace(diction["countries"],subset=['country','guarantor']).replace(diction["regions"],subset=['region'])
    df_borrower_cleaned = df_cleaned
    for country, borrower in diction['borrower_dict'].items():
        df_borrower_cleaned = df_borrower_cleaned.withColumn(
            "borrower",
            when(col("country") == country, borrower).otherwise(col("borrower"))
        )

    
    # Save cleaned data in XCom for next step
    rows = df_cleaned.collect()
    serialized_data = {"columns": df_cleaned.columns, "data": [row.asDict() for row in rows]}
    kwargs['ti'].xcom_push(key='df_cleaned_xcom', value=serialized_data)

    #kwargs['ti'].xcom_push(key='cleaned_data', value=df_cleaned.collect())

def mapping(diction,**kwargs):
    schema=StructType([StructField('end_of_period', StringType(), True), 
                    StructField('loan_number', StringType(), True), 
                    StructField('region', StringType(), True), 
                    StructField('country_code', StringType(), True), 
                    StructField('country', StringType(), True), 
                    StructField('borrower', StringType(), True), 
                    StructField('guarantor_country_code', StringType(), True), 
                    StructField('guarantor', StringType(), True), 
                    StructField('loan_type', StringType(), True), 
                    StructField('loan_status', StringType(), True), 
                    StructField('interest_rate', DoubleType(), True),
                    StructField('project_id', StringType(), True), 
                    StructField('project_name_', StringType(), True), 
                    StructField('original_principal_amount', DoubleType(), True), 
                    StructField('cancelled_amount', DoubleType(), True), 
                    StructField('undisbursed_amount', DoubleType(), True), 
                    StructField('disbursed_amount', DoubleType(), True), 
                    StructField('repaid_to_ibrd', DoubleType(), True), 
                    StructField('due_to_ibrd', DoubleType(), True),  
                    StructField('borrowers_obligation', DoubleType(), True), 
                    StructField('sold_3rd_party', DoubleType(), True), 
                    StructField('repaid_3rd_party', DoubleType(), True), 
                    StructField('due_3rd_party', DoubleType(), True), 
                    StructField('loans_held', DoubleType(), True), 
                    StructField('first_repayment_date', StringType(), True), 
                    StructField('last_repayment_date', StringType(), True), 
                    StructField('board_approval_date', StringType(), True)])
    global spark
    filtered_data_1 = kwargs['ti'].xcom_pull(key='df_cleaned_xcom')
    columns=filtered_data_1['columns']
    data=filtered_data_1['data']
    filtered_data=spark.createDataFrame(data,schema=schema)
    filtered_data.show()
    country_BK_map = create_map([lit(x) for pair in diction["country_bk"].items() for x in pair])
    borrower_BK_map = create_map([lit(x) for pair in diction["borrower_bk"].items() for x in pair])
    regions_Bk_map = create_map([lit(x) for pair in diction["regions_Bk"].items() for x in pair])
    loan_status_Bk_map = create_map([lit(x) for pair in diction["loan_status_bk"].items() for x in pair])
    loan_type_Bk_map = create_map([lit(x) for pair in diction["loan_type_bk"].items() for x in pair])


    df_null_filled=filtered_data.na.fill('not_specified',subset=['borrower','guarantor'])

    df_regions_bk=df_null_filled.withColumn("region_BK", regions_Bk_map[col("region")])
    df_countries_bk = df_regions_bk.withColumn("country_BK", country_BK_map[col("country")])
    df_guaranter_bk = df_countries_bk.withColumn("guarantor_BK", country_BK_map[col("guarantor")])
    df_borrower_bk= df_guaranter_bk.withColumn("borrower_BK",borrower_BK_map[col("borrower")])
    df_loan_status_bk = df_borrower_bk.withColumn("loan_status_BK",loan_status_Bk_map[col("loan_status")])
    df_loan_type_bk = df_loan_status_bk.withColumn("loan_type_BK",loan_type_Bk_map[col("loan_type")])
    df_loan_type_bk.show()
    from pyspark.sql import Window
    from pyspark.sql.functions import last
    import sys
    # Assuming df is your DataFrame and 'column_name' is the column you want to forward-fill
    window_spec = Window.orderBy('loan_number').rowsBetween(-sys.maxsize, 0)

    df_filled_project = df_loan_type_bk.withColumn(
        'project_name_',
        last('project_name_', ignorenulls=True).over(window_spec))
    df_final=df_filled_project.withColumn("repaid",col('repaid_to_ibrd')+col('repaid_3rd_party'))\
        .withColumn("due",col('due_to_ibrd')+col('due_3rd_party'))\
            .drop('repaid_to_ibrd','repaid_3rd_party','due_to_ibrd','due_3rd_party','sold_3rd_party','loans_held')

    df_final.show()
    rows = df_final.collect()
    serialized_data2 = {"columns": df_final.columns, "data": [row.asDict() for row in rows]}
    kwargs['ti'].xcom_push(key='mapped_data', value=serialized_data2)
    #kwargs['ti'].xcom_push(key='mapped_data', value=df_loan_type_bk.collect())



def write_to_sql(**kwargs):
    schema=StructType([StructField('end_of_period', StringType(), True), 
                    StructField('loan_number', StringType(), True), 
                    StructField('region', StringType(), True), 
                    StructField('country_code', StringType(), True), 
                    StructField('country', StringType(), True), 
                    StructField('borrower', StringType(), True), 
                    StructField('guarantor_country_code', StringType(), True), 
                    StructField('guarantor', StringType(), True), 
                    StructField('loan_type', StringType(), True), 
                    StructField('loan_status', StringType(), True), 
                    StructField('interest_rate', DoubleType(), True),
                    StructField('project_id', StringType(), True), 
                    StructField('project_name_', StringType(), True), 
                    StructField('original_principal_amount', DoubleType(), True), 
                    StructField('cancelled_amount', DoubleType(), True), 
                    StructField('undisbursed_amount', DoubleType(), True), 
                    StructField('disbursed_amount', DoubleType(), True),  
                    StructField('borrowers_obligation', DoubleType(), True),
                    StructField('first_repayment_date', StringType(), True), 
                    StructField('last_repayment_date', StringType(), True), 
                    StructField('board_approval_date', StringType(), True),
                    StructField('region_BK',StringType(),True),
                    StructField('country_BK',StringType(),True),
                    StructField('guarantor_BK',StringType(),True),
                    StructField('borrower_BK',StringType(),True),
                    StructField('loan_status_BK',StringType(),True),
                    StructField('loan_type_BK',StringType(),True),
                    StructField('Repaid',DoubleType()),
                    StructField("Due",DoubleType())])
    global spark
    data_final = kwargs['ti'].xcom_pull(key='mapped_data')
    columns=data_final['columns']
    data=data_final['data']
    df_final=spark.createDataFrame(data,schema=schema)

    url = "jdbc:sqlserver://192.168.1.6;databaseName=Loan_Data_try"
    table_name = "LoanData"
    username = 'maher'
    password = '12345'

    properties = {
        "user": username,
        'password': password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # Write final DataFrame to SQL Server
    df_final.write.jdbc(url=url, table=table_name, mode='append', properties=properties)

with DAG(
    dag_id='Loan_Data_preprocessing_gp',
    default_args=default_args,
    start_date=datetime(2024, 9, 26),
    tags=["Preprocessing",'Grad Project'],
    schedule_interval="@hourly",  # Run every hour
    catchup=False,  # Avoid backfilling for initial runs
) as dag:



    """
    start_spark_task = PythonOperator(
        task_id='start_spark_session',
        python_callable=create_spark_session,
        dag=dag
    )
    """

    initialize_offset_task = PythonOperator(
        task_id='initialize_offset',
        python_callable=initialize_offset,
        dag=dag
    )

    fetch_data_task = PythonOperator(
    task_id='fetch_data_from_api',
    provide_context=True,
    python_callable=fetch_data_from_api,
    dag=dag
    )

    filter_and_drop_task = PythonOperator(
        task_id='filter_and_drop_columns',
        provide_context=True,
        python_callable=filter_and_drop_columns,
        dag=dag
    )

    
    read_csv_task = PythonOperator(
        task_id='read_csv_cleaning_files',
        python_callable=read_csv_cleaning_files,
        dag=dag
    )

    clean_data_task = PythonOperator(
        task_id='clean_data'
        ,python_callable=clean_data,
        op_kwargs={'diction':read_csv_task.output},
        dag=dag
    )
    
    mapping_task = PythonOperator(
        task_id='mapping_task',
        python_callable=mapping,
        op_kwargs={'diction': read_csv_task.output},
        provide_context=True,
        dag=dag,
    )

    write_to_sql_task = PythonOperator(
            task_id='write_to_sql',
            python_callable=write_to_sql,
            provide_context=True,
            dag=dag
        )
    
    ###start_spark_task >>
    fetch_data_task >> filter_and_drop_task >> read_csv_task >> clean_data_task >> mapping_task >> write_to_sql_task
