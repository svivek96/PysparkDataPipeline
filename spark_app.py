from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def data():
    spark = SparkSession.builder.appName("Airflow_Assignment-1").config("spark.sql.warehouse.dir","gs://airflow_assignment_one/hive_data/").enableHiveSupport().getOrCreate()

    bucket = f"airflow_assignment_one"
    emp_data_path = f"gs://{bucket}/input_files/employee.csv"
    # dept_data_path = f"gs://{bucket}/input_files/department.csv"
    output_path =f"gs://{bucket}/output_files/"


    employee =spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",",").load(emp_data_path)
    # department =spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",",").load(dept_data_path)

    # Filter employee data
    filtered_employee = employee.filter(employee.salary > 60000) #Adjust the salary threshold as needed

    # Join datasets
    # joined_data = filtered_employee.join(department, "dept_id", "inner")

    filtered_employee.show(truncate=False)

    # Write output
    filtered_employee.write.csv(output_path, header=True)
    
    spark.sql("""CREATE DATABASE IF NOT EXISTS ASSIGNMENT_ONE""")
    
    HIVE_QUERY ="""CREATE TABLE IF NOT EXISTS ASSIGNMENT_ONE.EMPLOYEE (
                    emp_id int,
                    emp_name string,
                    dept_id int,
                    salary int)
                    stored as parquet
                    """
                    
    spark.sql(HIVE_QUERY)  
    filtered_employee.write.mode("append").format("hive").saveAsTable("ASSIGNMENT_ONE.EMPLOYEE")    
    spark.stop()
    
if __name__ == '__main__':
    data()
        
