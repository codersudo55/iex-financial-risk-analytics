from pyspark.sql import SparkSession

def ingest_from_s3(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("IngestRaw").getOrCreate()
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.write.parquet(output_path, mode="overwrite")
    print(f"Ingested raw data from {input_path} to {output_path}")

if __name__ == "__main__":
    ingest_from_s3("s3://bucket/raw/", "s3://bucket/parquet/raw/")
