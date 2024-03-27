import sys
from lib import DataManipulation, DataReader, Utils, logger
from pyspark.sql.functions import * 
from lib.logger import Log4j

if __name__ == '__main__':
    if len(sys.argv) < 2 :
        print("Please Specify the Environment")
        sys.exit(-1)

    job_run_env = sys.argv[1]

    print("Creating Spark Session")

    spark = Utils.get_spark_session(job_run_env)
    logger = Log4j(spark)

    logger.warn("Created Spark Session")

    orders_df = DataReader.read_orders(spark, job_run_env)

    orders_filtered = DataManipulation.filter_closed_orders(orders_df)

    customers_df = DataReader.read_customer(spark,job_run_env)

    joined_df = DataManipulation.join_orders_customer(orders_filtered,customers_df)
    aggregated_results = DataManipulation.count_orders_state(joined_df)
    aggregated_results.show()

    logger.info("this is the end of main")
