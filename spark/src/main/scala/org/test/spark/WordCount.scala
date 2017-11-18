package org.test.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args : Array[String]) = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    
    val rddText = sc.textFile("texts.txt")
    rddText.flatMap(line => line.split(" "))
    .map(word => (word,1))
    .reduceByKey(_ + _)
    .saveAsTextFile("count.txt")
    
  }
}