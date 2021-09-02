import sys
import re
import logging
applicationName='SALUTE_WITH_PYSPARK.py'
# create logger
logger = logging.getLogger(applicationName)
logging.basicConfig(level=logging.INFO)
logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
import random
import time
import argparse
from pyspark.sql import SparkSession, SQLContext, HiveContext
from pyspark import SparkConf, SparkContext

def randomizeGreeting(userName):
    y = ["Hi", "Hello", "greetings", "gooday", "Jambo", "What’s up?", "It’s a pleasure to meet you", "Pleased to meet you", "Did I pronounce your name correctly? "]
    x = random.randint(1, len(y))
    return y[x] +" "+ userName

    return
if __name__ == '__main__':
        spark = SparkSession.builder.enableHiveSupport().config("spark.sql.catalogImplementation", "hive").getOrCreate()
        # sqlContext = SQLContext.getOrCreate(spark.sparkContext)

        try:
            parser = argparse.ArgumentParser(prog='SALUTE',
                                             description='Send salutations to SXM Cluster with spark submit')
            parser.add_argument('--version', action='version', version='%(prog)s 0.1-EON-ALPHA')
            parser.add_argument('Spark_User', help='Spark user sending saluations with pyspark')

            args = parser.parse_args()
            sprk_User = args.Spark_User

            processStageName = "Randomizing salutations...."
            logger.info("%s start    %s" % (time.strftime("%Y%m%dT%T"), processStageName))
            greeting = randomizeGreeting(sprk_User)
            print("***** %s *****" % (greeting))
            logger.info("%s complete %s" % (time.strftime("%Y%m%dT%T"), processStageName))
        except Exception as e:
            errorMessage="%s Oops looks like something went wrong here. rootCause: %s " % (time.strftime("%Y%m%dT%T"),str(e))
            logger.error(errorMessage)  ## log the formatted error message
            # raise Exception(errorMessage)  ## throw the exception with appropriate message to caller program/method
        finally:
            processStageName = "In the final statement where we probably need to do some house keeping"
            logger.info("%s: Complete - %s stage. " % (time.strftime("%Y%m%dT%T"), processStageName))
