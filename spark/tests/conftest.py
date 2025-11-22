import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_fixture():
    spark = (
        SparkSession.builder
        .appName("BaseballAnalytics")
        .master("local[1]")
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33")
        .getOrCreate()
    )
    yield spark
