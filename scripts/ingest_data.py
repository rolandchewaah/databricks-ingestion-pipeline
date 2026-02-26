import os
from pyspark.sql import SparkSession

def main():
    print("Starting ingestion...")

    input_path = os.environ["INPUT_PATH"]
    schema_location = os.environ["SCHEMA_LOCATION"]
    checkpoint_location = os.environ["CHECKPOINT_LOCATION"]
    target_table = os.environ["TARGET_TABLE"]

    spark = SparkSession.builder.appName("IngestionJob").getOrCreate()

    df = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", schema_location)
            .option("header", "true")
            .option("inferColumnTypes", "true")
            .load(input_path)
    )

    query = (
        df.writeStream
          .format("delta")
          .option("checkpointLocation", checkpoint_location)
          .trigger(availableNow=True)
          .toTable(target_table)
    )

    # Critical for jobs so run doesn't exit early
    query.awaitTermination()

    print("Ingestion completed.")

if __name__ == "__main__":
    main()