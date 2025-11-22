import os

from pyspark.sql import SparkSession, Window, DataFrame, functions as F

from utils import read_mysql_table, save_to_s3


# Some setup I needed because of running locally. Could've ran inside a Docker instead
os.environ["HADOOP_HOME"] = r"somelocalpath\hadoop"
os.environ["PATH"] += r";somelocalpath\hadoop\bin"

def get_average_salaries(fielding_df: DataFrame, salaries_df: DataFrame):
    INFIELD_POSITIONS = ["1B", "2B", "3B", "SS"]

    players_positions = fielding_df.withColumn(
            "group",
            F.when(F.col("POS").isin(INFIELD_POSITIONS), "infielder")
            .when(F.col("POS") == "P", "pitcher")
        ).where(
            F.col("group").isNotNull()
        ).select(
            'playerID', 'yearID', 'group'
        )
    
    salary_positions = salaries_df.alias('a').join(
            players_positions.alias('b'), ["playerID", "yearID"],
        ).select(
            'a.salary', 'b.yearID', 'b.group'
        )

    avg_salary = (
        salary_positions
        .groupBy("yearID", "group")
        .agg(F.avg("salary").alias("avg_salary"))
        .orderBy("yearID", "group")
    )

    avg_salary_pivot = (
        avg_salary
        .groupBy("yearid")
        .pivot("group", ["infielder", "pitcher"])
        .agg(F.first("avg_salary"))
        .orderBy("yearid")
    )

    avg_salary_pivot_formatted = avg_salary_pivot.withColumn(
            "Fielding", F.format_number("infielder", 0)
        ).withColumn(
            "Pitching", F.format_number("pitcher", 0)
        ).select(
            F.col('yearid').alias('Year'), 'Fielding', 'Pitching'
        )
        
    return avg_salary_pivot_formatted
    
def get_allstar_stats(hof_df: DataFrame, pitching_df: DataFrame, allstar_df: DataFrame):
    hof_players = (
        hof_df
        .filter(F.col("inducted") == "Y")
        .select("playerID", "yearID")
        .withColumnRenamed("yearID", "induction_year")
    )

    pitching_with_era = pitching_df.select("playerID", "yearID", "ERA")

    hof_pitchers = hof_players.join(
        pitching_with_era, 'playerID'
    )
    
    allstar_appearances = allstar_df.select(
            "playerID", "yearID"
        ).groupBy(
            "playerID", "yearID"
        ).agg(
            F.count("*").alias("allstar_appearances")
        )

    hof_pitchers_allstar = hof_pitchers.join(
        allstar_appearances, ['playerID', 'yearID'], 'left'
        ).withColumn(
            "allstar_appearances", F.coalesce(F.col("allstar_appearances"), F.lit(0))
        )

    result = (
        hof_pitchers_allstar
        .groupBy("playerID")
        .agg(
            F.min(F.col("induction_year")).alias("Hall of Fame Induction Year"),
            (F.sum(F.when(F.col("allstar_appearances") > 0, F.col("ERA")).otherwise(0)) /
            F.sum(F.when(F.col("allstar_appearances") > 0, 1).otherwise(0))).alias("ERA"),
            F.sum("allstar_appearances").alias("# All Star Appearances")
        )
    )

    result_formatted = result.withColumnRenamed(
        "playerID", "Player"
    ).withColumn(
        "ERA", F.round(F.col("ERA"), 2)
    ).select(
        "Player", "ERA", "# All Star Appearances" , "Hall of Fame Induction Year"
    ).orderBy("playerID")

    return result_formatted

def get_top10_stats(pitching_df: DataFrame, pitching_post_df: DataFrame):
    regular_season = (
        pitching_df
        .withColumn("win_loss", F.col("W") / (F.col("W") + F.col("L")))
        .select("playerID", "yearID", "ERA", "win_loss")
    )

    post_season = (
        pitching_post_df
        .withColumn("post_win_loss", F.col("W") / (F.col("W") + F.col("L")))
        .select("playerID", "yearID", "ERA", "post_win_loss")
        .withColumnRenamed("ERA", "post_ERA")
    )

    window = (
        Window.partitionBy("yearID")
            .orderBy(F.col("ERA").asc())
    )

    top_pitchers = (
        regular_season
        .withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= 10)
        .drop("rank")
    )

    joined = top_pitchers.join(
        post_season, ["playerID", "yearID"], "left"
    )

    result = (
        joined
        .groupBy("yearID")
        .agg(
            F.round(F.avg("ERA"), 2).alias("avg_regular_ERA"),
            (100*F.round(F.avg("win_loss"), 2)).cast('int').alias("avg_regular_winloss"),
            (F.sum(F.when(F.col("post_ERA").isNotNull(), F.col("post_ERA")).otherwise(0)) /
            F.sum(F.when(F.col("post_ERA").isNotNull(), 1).otherwise(0))).alias("avg_post_ERA"),
            (100*F.sum(F.when(F.col("post_win_loss").isNotNull(), F.col("post_win_loss")).otherwise(0)) /
            F.sum(F.when(F.col("post_win_loss").isNotNull(), 1).otherwise(0))).cast('int').alias("avg_post_winloss"),
        )
        .orderBy("yearID")
    )

    result_formatted = result.select(
        F.col("yearID").alias("Year"),
        F.col("avg_regular_ERA").alias("Regular Season ERA"),
        F.col("avg_regular_winloss").alias("Regular Season Win/Loss"),
        F.col("avg_post_ERA").alias("Post-season ERA"),
        F.col("avg_post_winloss").alias("Post-season Win/Loss"),
    )

    return result_formatted

def get_first_last_teams(teams_df: DataFrame):
    window = Window.partitionBy("yearID")
    teams_with_max = teams_df.withColumn("max_rank", F.max("rank").over(window))
    teams_first_last = teams_with_max.filter(
        (F.col("rank") == 1) | (F.col("rank") == F.col("max_rank"))
    )

    result = teams_first_last.select(
        F.col("teamID").alias("Team ID"),
        F.col("yearID").alias("Year"),
        F.col("rank").alias("Rank"),
        F.col("AB").alias("At Bats")
    ).orderBy("Year", "Rank")

    return result

def main():
    # Init with MySQL connector. Master is "local" but should be yarn/k8s ideally
    spark = (
        SparkSession.builder
        .appName("BaseballAnalytics")
        .master("local[2]")
        .config("spark.ui.port", "4050")
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33")
        .getOrCreate()
    )
    
    salaries_df = read_mysql_table(spark, "Salaries")
    fielding_df = read_mysql_table(spark, "Fielding")
    pitching_df = read_mysql_table(spark, "Pitching")
    pitching_post_df = read_mysql_table(spark, "PitchingPost")
    allstar_df = read_mysql_table(spark, "AllstarFull")
    hof_df = read_mysql_table(spark, "HallOfFame")
    teams_df = read_mysql_table(spark, "Teams")

    save_to_s3(get_average_salaries(fielding_df, salaries_df), 'out/average_salaries.csv')
    save_to_s3(get_allstar_stats(hof_df, pitching_df, allstar_df), 'out/hall_of_fame_all_star_pitchers.csv')
    save_to_s3(get_top10_stats(pitching_df, pitching_post_df), 'out/pitching.csv')
    save_to_s3(get_first_last_teams(teams_df), 'out/rankings.csv')
    
if __name__ == '__main__':
    main()
