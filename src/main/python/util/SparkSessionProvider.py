from pyspark.sql import SparkSession


class SparkSessionProvider:
    def createSparkSession(self):
        return SparkSession.builder.appName("pyspark-example").getOrCreate
