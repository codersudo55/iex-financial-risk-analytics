from pyspark.sql import SparkSession

def transform_curated(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("TransformCurated").getOrCreate()
    df = spark.read.parquet(input_path)
    curated = df.dropna()
    curated.write.parquet(output_path, mode="overwrite")
    print(f"Curated data written to {output_path}")

if __name__ == "__main__":
    transform_curated("s3://bucket/parquet/raw/", "s3://bucket/parquet/curated/")
