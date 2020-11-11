import sys
 
from pyspark import SparkContext, SparkConf
 
if __name__ == "__main__":
	
	# create Spark context with necessary configuration
	sc = SparkContext("local","PySpark Word Count Exmaple")
	
	dataFile = open("C:\\Users\\Zduss\\Spark\\learning\\data\\0.txt")
	wordList = []
	for line in dataFile:
		content = line.split('\t')
		for i in range(9, len(content)):
			wordList.append(content[i].rstrip())

	# read data from text file and split each line into words
	words = sc.parallelize(wordList)
	
	# count the occurrence of each word
	wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
	
	# save the counts to output
	wordCounts.saveAsTextFile("C:\\Users\\Zduss\\Spark\\learning\\out")