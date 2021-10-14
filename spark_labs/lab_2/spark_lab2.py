import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import sys


def df_processing(airlines_df, airports_df, flights_df):
    enriched_flights_df = flights_df.select(flights_df.ORIGIN_AIRPORT, flights_df.AIRLINE, flights_df.DEPARTURE_DELAY) \
        .join(airports_df.select(F.col('AIRPORT').alias('AIRPORT_NAME'), 'IATA_CODE'),
              airports_df.IATA_CODE == flights_df.ORIGIN_AIRPORT, 'inner') \
        .join(F.broadcast(airlines_df.select(F.col('AIRLINE').alias('AIRLINE_NAME'), 'IATA_CODE')),
              airlines_df.IATA_CODE == flights_df.AIRLINE, 'inner') \
        .drop('IATA_CODE')

    max_delayer_airline_per_airport = enriched_flights_df\
        .groupBy(F.col('ORIGIN_AIRPORT'), F.col('AIRLINE'))\
        .agg(
            (F.max(F.col('AIRPORT_NAME'))).alias('AIRPORT_NAME'),
            (F.max(F.col('AIRLINE_NAME'))).alias('AIRLINE_NAME'),
            (F.mean(F.col('DEPARTURE_DELAY'))).alias('AVG_AIRLINE_DEPARTURE_DELAY')
        ).withColumn("rn", F.row_number() \
                            .over(Window.partitionBy('ORIGIN_AIRPORT') \
                            .orderBy(F.col('AVG_AIRLINE_DEPARTURE_DELAY').desc()))) \
        .where(F.col('rn') == 1)

    delays_per_airport_df = flights_df\
        .groupBy(F.col('ORIGIN_AIRPORT')) \
        .agg(
            F.mean(F.col('DEPARTURE_DELAY')).alias('AVG_DEPARTURE_DELAY'),
            F.max(F.col('DEPARTURE_DELAY')).alias('MAX_DEPARTURE_DELAY')
        )

    final_result_df = delays_per_airport_df\
        .join(max_delayer_airline_per_airport, 'ORIGIN_AIRPORT', 'inner')\
        .select(
            delays_per_airport_df.ORIGIN_AIRPORT,
            'AIRPORT_NAME',
            'AVG_DEPARTURE_DELAY',
            'MAX_DEPARTURE_DELAY',
            'AIRLINE',
            'AIRLINE_NAME',
            'AVG_AIRLINE_DEPARTURE_DELAY'
        )

    return final_result_df


if __name__ == "__main__":

    source_prefics = sys.argv[1]
    outcome_path = sys.argv[2]
    path_to_airlines = source_prefics + 'airlines.csv'
    path_to_airports = source_prefics + 'airports.csv'
    path_to_flights = source_prefics + 'flights.csv'

    spark = SparkSession.builder.appName('lab_2')\
                        .master("local[*]").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partition", 20)

    airlines_df = spark.read.format('CSV')\
        .option('inferschema', True)\
        .option('sep', ',')\
        .option('header', True)\
        .load(path_to_airlines)

    airports_df = spark.read.format('CSV')\
        .option('inferschema', True)\
        .option('sep', ',')\
        .option('header', True)\
        .load(path_to_airports)

    flights_df = spark.read.format('CSV') \
        .option('inferschema', True)\
        .option('sep', ',')\
        .option('header', True)\
        .load(path_to_flights)

    final_result_df = df_processing(airlines_df, airports_df, flights_df)
    final_result_df.repartition(10).write.format("json") \
        .mode("overwrite")\
        .save(outcome_path)
