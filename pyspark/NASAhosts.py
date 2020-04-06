import os
import sys
 
os.environ["SPARK_HOME"] = "/usr/spark2.3/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
# In below two lines, use /usr/bin/python2.7 if you want to use Python 2
os.environ["PYSPARK_PYTHON"] = "/usr/local/anaconda/bin/python" 
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/anaconda/bin/python"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.7-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")

# import the libraries
import time
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    
     ## first initialise sparkconf and sparkcontext object 
     conf = SparkConf().setAppName("NASAhosts").setMaster("local[1]")
     sc = SparkContext(conf = conf)
    
     # then we load the 2 log files 
     julyFirstLogs = sc.textFile("udemy_pyspark/nasa_19950701.tsv")
     print(julyFirstLogs.take(2))
     augustFirstLogs = sc.textFile("udemy_pyspark/nasa_19950801.tsv")
     print(augustFirstLogs.take(2))   
     ##  next we split the each of the RDD alg the tabs and take the first instance of evry record
     ## the first record give us he host namem
    
     julyFirstHosts = julyFirstLogs.map(lambda line: line.split("\t")[0])
     print(julyFirstHosts.take(5))
     augustFirstHosts = augustFirstLogs.map(lambda line: line.split("\t")[0])
     print(augustFirstHosts.take(5))
    
     ## find out the common URLS or hosts used   for july and august 
     intersection_dff = julyFirstHosts.intersection(augustFirstHosts)
    
     cleanedhistlist = intersection_dff.filter(lambda  host: host !="host")
     cleanedhistlist.saveAsTextFile("out/cleanhist.csv")  