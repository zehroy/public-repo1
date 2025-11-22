import pyspark.sql.functions as F

from src.main import get_average_salaries


def test_average_salaries(spark_fixture):
    fielding_df = spark_fixture.createDataFrame([
        ("p1", 2000, "1B"),
        ("p2", 2000, "P"),
    ], ["playerID", "yearID", "POS"])

    salaries_df = spark_fixture.createDataFrame([
        ("p1", 2000, 1000000),
        ("p2", 2000, 2000000),
    ], ["playerID", "yearID", "salary"])

    df = get_average_salaries(fielding_df, salaries_df)
    result = df.collect()[0]

    assert result["Year"] == 2000
    assert result["Fielding"] == "1,000,000"
    assert result["Pitching"] == "2,000,000"