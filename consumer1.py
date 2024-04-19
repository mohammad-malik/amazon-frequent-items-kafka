from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, desc, json_tuple


def process_stream(rdd):
    if not rdd.isEmpty():
        # Convert RDD to DataFrame
        spark = SparkSession.builder.appName(
            "AmazonReviewStreamProcessor"
        ).getOrCreate()
        df = spark.read.json(rdd)

        # Explode 'also_buy' into separate rows for easier analysis
        df = df.withColumn("also_buy", explode(split(df["also_buy"], ",")))

        # Example aggregation: Count most frequently bought together products
        also_buy_frequencies = \
            df.groupBy("also_buy").count().orderBy(desc("count"))

        # Example insight: Most popular categories
        category_popularity = (
            df.withColumn("category", explode(split(df["category"], ",")))
            .groupBy("category")
            .count()
            .orderBy(desc("count"))
        )

        # Print out the top 10 results for each to the console
        print("Most Frequently Bought Together Products:")
        also_buy_frequencies.show(10)

        print("Most Popular Categories:")
        category_popularity.show(10)

        # More complex processing might be added here,


def main():
    # Create a local Spark context with two threads (might be changed).
    sc = SparkContext("local[2]", "AmazonReviewStreamProcessor")
    ssc = StreamingContext(
        sc, 10
    )  # Create a Streaming Context with batch interval of 10 seconds

    # Assuming data is streamed through a socket
    dstream = ssc.socketTextStream("localhost", 9999)

    # Apply processing function to each RDD in our DStream
    dstream.foreachRDD(process_stream)

    ssc.start()  # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate


if __name__ == "__main__":
    main()
