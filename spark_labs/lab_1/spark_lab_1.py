import pyspark.sql.functions as F
from pyspark.sql.window import Window


path_to_airlines = 'gs://procamp-test/datasets/airlines.csv'
path_to_airports = 'gs://procamp-test/datasets/airports.csv'
path_to_flights = 'gs://procamp-test/datasets/flights.csv'

outcome_path_task_1 = "gs://procamp-test/datasets/reports/task1/"
outcome_path_task_2_1 = "gs://procamp-test/datasets/reports/task2_1/"
outcome_path_task_2_2 = "gs://procamp-test/datasets/reports/task2_2/"


airlines_df = spark.read.format('CSV')\
                .option('inferschema', True) \
                .option('sep', ',')\
                .option('header', True)\
                .load(path_to_airlines).cache()
airports_df = spark.read.format('CSV')\
                .option('inferschema', True) \
                .option('sep', ',')\
                .option('header', True)\
                .load(path_to_airports).cache()
flights_df = spark.read.format('CSV')\
                .option('inferschema', True) \
                .option('sep', ',')\
                .option('header', True)\
                .load(path_to_flights).cache()



# Task 1
popular_flifhts_df = flights_df.groupBy( F.col('MONTH'), F.col('DESTINATION_AIRPORT')).count()

popular_flifhts_df = popular_flifhts_df.withColumn("row_number", 
                                                    F.row_number().over(Window.partitionBy('MONTH').orderBy(F.col('count').desc())))
                                                    
report_df = popular_flifhts_df.where(popular_flifhts_df.row_number ==1).select(F.col('DESTINATION_AIRPORT').alias('dest_airport'), F.col('count').alias('max_count'))


report_df = airports_df.join(report_df, report_df.dest_airport == airports_df.IATA_CODE, how = 'right')\
                     .select(F.coalesce(F.col('AIRPORT'), F.col('dest_airport')).alias('dest_airport'), F.col('max_count'))


report_df.coalesce(1).write.format("csv")\
                     .option("header", "true")\
                     .option("delimiter", "\t")\
                     .option("mode", "overwrite").csv(outcome_path_task_1)



# Task 2
perc_canceled_df = flights_df.groupBy(F.col('AIRLINE').alias('airline'), F.col('ORIGIN_AIRPORT').alias('origin_airport'))\
                             .agg(F.count(F.col('ORIGIN_AIRPORT')).alias('origin_counts'), 
                                  F.sum(F.col('CANCELLED')).alias('canceled_counts'))
                                  
perc_canceled_df = perc_canceled_df.withColumn('cancel_percentage', F.col('canceled_counts')/F.col('origin_counts'))\
                                   .withColumn('processed_counts', F.col('origin_counts') - F.col('canceled_counts'))


report_df = perc_canceled_df.join(airports_df, perc_canceled_df.origin_airport == airports_df.IATA_CODE, how = 'left')\
                     .join(airlines_df, perc_canceled_df.airline == airlines_df.IATA_CODE, how = 'left')\
                     .select(airlines_df.AIRLINE.alias('airline_name'),
                             F.coalesce(F.col('AIRPORT'), F.col('origin_airport')).alias('origin_airport_name'),
                             F.col('cancel_percentage'),
                             F.col('canceled_counts'),
                             F.col('processed_counts'))



report_df.where(F.col('origin_airport_name') != 'Waco Regional Airport')\
         .coalesce(1).write.format("json")\
         .option("mode", "overwrite").json(outcome_path_task_2_1)
                     
report_df.where(F.col('origin_airport_name') == 'Waco Regional Airport')\
         .coalesce(1).write.format("csv")\
         .option("header", "true")\
         .option("delimiter", ",")\
         .option("mode", "overwrite").csv(outcome_path_task_2_2)
