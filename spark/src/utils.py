import shutil
import os
import glob
from uuid import uuid4

from pyspark.sql import SparkSession, DataFrame


jdbc_url = "jdbc:mysql://localhost:3306/lahman2016"
db_props = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

def read_mysql_table(spark: SparkSession, table_name: str):
    return (spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", db_props["user"])
        .option("password", db_props["password"])
        .option("driver", db_props["driver"])
        .load()
        )

def save_to_s3(df: DataFrame, path: str):
    output_folder = f"out/{uuid4()}"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_folder)
    part_file = glob.glob(os.path.join(output_folder, "part-*.csv"))[0]
    shutil.move(part_file, path)
    shutil.rmtree(output_folder)