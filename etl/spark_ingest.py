from pyspark.sql import SparkSession

def load_iex_data(input_path: str):
    spark = SparkSession.builder.appName("IEXIngestion").getOrCreate()
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.show(10)
    return df

if __name__ == "__main__":
    input_path = "data/raw/iex/*.csv"
    df = load_iex_data(input_path)
