from main.python.conf.AppConf import AppConf
from main.python.model.Process import *
from main.python.util.SparkSessionProvider import SparkSessionProvider

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Need 4 arguments ", file=sys.stderr)
        sys.exit(-1)

appconfig = AppConf(str(sys.argv[1]), str(sys.argv[2]), str(sys.argv[3]), str(sys.argv[4]))
spark = SparkSessionProvider
sparkSession = spark.createSparkSession()
try:
    Process.run(appconfig, sparkSession)
except Exception as e:
    raise e
