from pyspark.sql import SparkSession
from delta.tables import DeltaTable


def init_euroleague_spark_session():
    """
    Initialize a spark session for euroleague data and return the session object.
    """
    spark = SparkSession.builder \
        .appName("Euroleague Delta Medallion Pipeline") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()

    # Create a database for Euroleague tables
    spark.sql("CREATE DATABASE IF NOT EXISTS euroleague")

    return spark


def write_delta(table_df, table_name, mode = None):
    """
    Writes the provided `table_df` to `table_name`. Uses 'overwrite' if the table doesn't exist, otherwise uses 'append'.
    When using 'append', drops duplicates after the final write.
    """
    # ensure that table df contains at least one record before writing
    if table_df is None:
        print(f"Table is empty. Nothing to write into '{table_name}'.")
        return

    # determine write mode if not specified
    if not mode:
        # extract spark sessioin from table df
        spark = table_df.sparkSession
        # use appropriate mode depending on whether table exists
        if not spark.catalog.tableExists(table_name):
            mode = "overwrite"
            print(f"Table '{table_name}' does not exist. Using mode 'overwrite'.")
        else:
            mode = "append"
            print(f"Table '{table_name}' exists. Using mode 'append'.")

    # write table
    table_df.write.format("delta").mode(mode).saveAsTable(table_name)
    print(f"Table '{table_name}' written successfully with mode '{mode}'")

    # Drop duplicates after append
    if mode == "append":
        spark = table_df.sparkSession
        df = spark.table(table_name)
        # Drop duplicates based on all columns
        df_deduped = df.dropDuplicates()
        df_deduped.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"Duplicates dropped from '{table_name}' after append.")