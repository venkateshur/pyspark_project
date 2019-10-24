class AppConf(object):
    def __init__(self, inputBasePath, inputFilesPath, outputFilesPath, outputFileName):
        """
        :type ouputFileName: object
        """
        self.inputBasePath = inputBasePath
        self.inputFilesPath = inputFilesPath
        self.outputFilesPath = outputFilesPath
        self.outputFileName = outputFileName
