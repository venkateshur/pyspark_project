

def textReader(spark, inputPath):
    """

    :param spark:
    :param inputPath:
    :return:
    """
    return spark.read.textFile(inputPath)


def csvReader(spark, inputPath):
    """

    :param spark:
    :param inputPath:
    :return:
    """
    return spark.read.option("header", "true").csv(inputPath)
