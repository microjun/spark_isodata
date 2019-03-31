package com.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by IntelliJ IDEA.
  * User: Kekeer
  * Date: 2019/3/27
  * Time: 12:50
  */
object WordCount {

    val LOCAL_HOST: String = "hdfs://" + "localhost" + ":9000"
    val MASTER: String = "local"
    //    val MASTER: String = "spark://172.16.23.24:7077"

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("WordCount").setMaster(MASTER)
        val sc = new SparkContext(conf)
        val file = sc.textFile(LOCAL_HOST + "/user/test/data.txt")

        val rdd = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

        rdd.saveAsTextFile(LOCAL_HOST + "/user/test/output")

        rdd.collect().foreach(data => println(data))
        sc.stop()
    }

}
