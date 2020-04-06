# bigdata
# Big data case studies , including Hive , mapreduce , spark and scala


####Hive 
## A project of  writing HQL queries in Hue , and visualisation through tableau 
https://github.com/asmitaece88/bigdata/blob/master/hive/Hive_assignment_project.docx


## Creating and storing data into hive tables from spark-sql
https://github.com/asmitaece88/bigdata/blob/master/hive/spark-sql_hive.docx


## mapreduce 
### Using unix  command as map-reduce functionality to check the count of unique words in a text document ,
## details documented in word document 
https://github.com/asmitaece88/bigdata/tree/master/mapreduce/unix/mapreduce_using_unix.docx

### Spark 
## Note: All spark commands are run in scala prompt through spark-shell
### Check the comments and the results marked with //  in the corresponding text files ,just for documentation and understanding purpose 

## Example of how we can convert a  variable to broadcast ,  and find out the count of each grammer elements from a list
https://github.com/asmitaece88/bigdata/blob/master/broadcast_dictionary_example.txt


## Example of usage of broadcast variable  in removing common words from a text file 
https://github.com/asmitaece88/bigdata/blob/master/broadcast_usage_removal_common_words.txt
# screenshot of the data set uploaded in cloudxlab server 
https://github.com/asmitaece88/bigdata/blob/master/big_text_file.PNG

### Usage of accumulator , in reading blank spaces in a  large as well as small file
https://github.com/asmitaece88/bigdata/blob/master/spark_accumulator_practice_examples.txt
## dataset resource :small_text_file.png

## Usage of custom accumulator with examples 
https://github.com/asmitaece88/bigdata/blob/master/Custom_accumulator.txt

#### #Uploading code and step  by step screenshots  of pushing data from various files ,  from a kafka producer to kafka consumer 

https://github.com/asmitaece88/bigdata/blob/master/spark-streaming/pushing-file-data/Pushing_data_using_kafka_topic.docx

### document  consisting of steps followed to check spark streaming by creating a server and a listener through unix utility netcat 
https://github.com/asmitaece88/bigdata/blob/master/spark-streaming/word-count-spark-streaming/Wordcount_spark_streaming.docx


### Spark streaming integrated with kafka 
## Uploading scala code for calculating and showing word count generated  by kafka producer , every 2 seconds 
https://github.com/asmitaece88/bigdata/blob/master/spark-streaming/spark-streaming-using-kafka/KafkaWordCount.scala
## Uploading steps executed for  creating kafka producer , checking through kafka consumer and finally using spark streaming to get the count of unique words evry 2 seconds
https://github.com/asmitaece88/bigdata/blob/master/spark-streaming/spark-streaming-using-kafka/Wordcount-kafka.docx


### building an ALS recommendation engine  in scala 
## code and 3 data files , used for building spark alternating least squares recommendation engine
https://github.com/asmitaece88/bigdata/tree/master/spark-ml


### loading json , text files into dataframes and querying through sql .
## usage of encoders , inference of reflection
https://github.com/asmitaece88/bigdata/blob/master/spark-sql-examples/spark-sql-assignments.docx



### Node js 

### Demonstration  of using nodejs  and socket.IO to listen -to send and receive any user messages
https://github.com/asmitaece88/bigdata/blob/master/socket_IO.docx

## Pyspark
## demonstration of a simple Pyspark queue RDD streaming 
https://github.com/asmitaece88/bigdata/blob/master/pyspark/Pyspark_streaming_udemy.ipynb
## demontsration of a in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
## in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
##  Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
