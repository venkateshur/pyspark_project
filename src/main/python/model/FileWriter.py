def csvWriter(inDf, outputPath):
    """

    :param inDf:
    :param outputPath:
    """
    inDf.write.mode("overwrite").save(outputPath)
