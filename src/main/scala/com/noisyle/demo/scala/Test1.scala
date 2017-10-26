package com.noisyle.demo.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWC {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkWC").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val lines = sc.textFile(args(0))

    val words: RDD[String] = lines.flatMap(_.split(" "))
    val paired: RDD[(String, Int)] = words.map((_, 1))
    val reduced: RDD[(String, Int)] = paired.reduceByKey(_+_)
    val res: RDD[(String, Int)] = reduced.sortBy(_._2, false)

    println(res.collect().toBuffer)
    //res.saveAsTextFile(args(1))

    sc.stop()
  }
}
