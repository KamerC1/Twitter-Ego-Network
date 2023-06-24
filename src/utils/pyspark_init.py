def set_pySpark(memory = "10g"):
    import os, sys
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
    os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    from pyspark.sql import SparkSession

    spark = (SparkSession.builder
             .master('local[*]')
             .config("spark.executor.memory", memory)
             .config("spark.driver.memory", memory)
             .config("spark.executor.instances", "21")
             .config("spark.default.parallelism", "220")
             .config("spark.driver.cores", "5")
             .config("spark.executor.cores", "5")
             .appName('my-app')
        .getOrCreate())

    from pyspark.shell import sqlContext
    sqlContext.sql("set spark.sql.shuffle.partitions=220")
    spark.sparkContext.setLogLevel("WARN")
    

    spark.sparkContext.getConf().getAll()