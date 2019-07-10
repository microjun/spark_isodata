package com.spark

import java.awt.Color
import java.awt.image.BufferedImage
import java.io._
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date
import javax.imageio.ImageIO

import com.spark.util.{HdfsUtil, RgbUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

/**
  * Created by IntelliJ IDEA.
  * User: Kekeer
  * Date: 2019/3/27
  * Time: 14:23
  */
object ISODATA {

    val HADOOP_HOST: String = "hdfs://" + "219.245.196.115" + ":9000"
    val MASTER: String = "spark://219.245.196.115:7077"
    var INPUT_DIR: String = "/keke/text/"
    var INPUT_FILE: String = "image1006"
    var INPUT_PATH: String = INPUT_DIR + INPUT_FILE

    val OUTPUT_PATH: String = "/keke/spark/output"
    val JAR_PATH: String = "/root/Downloads/ISODATA.jar"

    var K: Int = 2 // 初始聚类数
    val k: Int = 3 // 期望得到的聚类数
    var On: Int = 100 // 每个类别中最小样本数，小于这个值，则取消
    val Oc: Double = 20 // 两个类别间最小距离，小于这个值，则合并
    val Os: Double = 10 // 一个类别中样本特征中最大标准差，大于这个值，则分裂
    val I: Int = 20 // 迭代运行的最大次数


    def main(args: Array[String]) {

        kmeans(10)
    }


    def kmeans(instance: Int): Unit = {

        var new_k_center: ArrayBuffer[Array[Double]] = new ArrayBuffer[Array[Double]](K) // 各聚类中心
        var old_k_center: ArrayBuffer[Array[Double]] = new ArrayBuffer[Array[Double]](K) // 各聚类中心
        var new_k_n: ArrayBuffer[Int] = new ArrayBuffer[Int](K) // 每个聚类对应的样本个数
        var new_k_dist: ArrayBuffer[Double] = new ArrayBuffer[Double](K) // 各聚类域中模式样本与各聚类中心间的平均距离
        var new_total_dist: Double = 0.0 // 样本和其对应聚类中心的总平均距离
        new_k_n ++= new Array[Int](K)
        new_k_dist ++= new Array[Double](K)

        val conf = new SparkConf()
            .setJars(List(JAR_PATH))
            .set("spark.driver.memory", "4g")
            .set("spark.executor.memory", "6g")
            .set("spark.executor.cores", "4")
            .set("spark.executor.instances", instance + "")
            .set("spark.cores.max", instance * 4 + "")
            .set("spark.default.parallelism", instance * 4 + "")

        val spark = SparkSession.builder()
            .appName("ISODATA")
            .master(MASTER)
            .config(conf)
            .getOrCreate

        var time1: Long = 0
        var time2: Long = 0
        var begintime: Long = 0
        var endtime: Long = 0

        begintime = System.currentTimeMillis()
        val image_rdd = spark.sparkContext.textFile(HADOOP_HOST + INPUT_PATH, 40).map(line => {
            val parts = line.split(" ").map(_.toDouble)
            Array[Array[Double]](Array[Double](parts(2), parts(3)), Array[Double](parts(0), parts(1)))

        }).cache()

        val takeArr = image_rdd.takeSample(withReplacement = false, K)
        for (item <- takeArr) {
            new_k_center += item(0)
        }

        var iter: Int = 0
        var dist: Double = 1.0
        var cancel_flag: Int = 0

        var new_center_broad: Broadcast[ArrayBuffer[Array[Double]]] = spark.sparkContext.broadcast(new_k_center)
        var old_center_broad: Broadcast[ArrayBuffer[Array[Double]]] = spark.sparkContext.broadcast(old_k_center)

        time1 = System.currentTimeMillis()
        breakable(
            while (true) {

                // 判断是否需要重新随机取聚类中心
                if (cancel_flag == 1) {
                    new_k_center.clear()
                    val takeArr = image_rdd.takeSample(withReplacement = false, K)
                    for (item <- takeArr) {
                        new_k_center += item(0)
                    }
                    new_k_n.clear()
                    new_k_n ++= new Array[Int](K)
                    new_k_dist.clear()
                    new_k_dist ++= new Array[Double](K)
                    cancel_flag = 0
                }

                new_center_broad = spark.sparkContext.broadcast(new_k_center)
                val newPoint = image_rdd.mapPartitions(item => closestCenter(item, new_center_broad)).reduceByKey((x, y) => (addVector(x._1, y._1), x._2 + y._2)).collect()

                breakable(
                    for (iter <- newPoint) {
                        if (iter._2._2 < On) {
                            K -= 1
                            cancel_flag = 1
                            break
                        }
                    }
                )
                // 更新聚类中心
                old_k_center = new_k_center.clone()
                dist = 0.0
                for (item <- newPoint) {
                    val nowVector = item._2._1
                    val nowN = item._2._2
                    dist += euclidDist(divVector(nowVector, nowN), new_k_center(item._1))
                    new_k_center(item._1) = nowVector
                    new_k_n(item._1) = nowN
                }

                if (dist < 0.01 || iter >= I) {
                    break()
                }


                val operation: Int = judgeOperation(iter)
                if (operation == 0) {

                    new_center_broad = spark.sparkContext.broadcast(new_k_center)
                    old_center_broad = spark.sparkContext.broadcast(old_k_center)
                    val pointDist = image_rdd.mapPartitions(item => closestCenter(item, old_center_broad)).mapPartitions(part => centerDist(part, new_center_broad)).reduceByKey((x, y) => (addVector(x._1, y._1), x._2 + y._2)).collect()

                    var total_o_dist = 0.0
                    var total_n = 0
                    for (iter <- pointDist) {
                        total_o_dist += iter._2._2
                        new_k_dist(iter._1) = iter._2._2 / new_k_n(iter._1)
                        total_n = new_k_n(iter._1)
                    }
                    new_total_dist = total_o_dist / total_n

                    for (point <- pointDist) {
                        val now_index: Int = point._1
                        val now_vec_arr: Array[Double] = sqrtDivVector(point._2._1, new_k_n(point._1))
                        var now_max: Double = Double.MinValue
                        var now_max_index: Int = 0
                        for (j <- now_vec_arr.indices) {
                            if (now_vec_arr(j) > now_max) {
                                now_max = now_vec_arr(j)
                                now_max_index = j
                            }
                        }

                        if (now_max > Os) {
                            if (new_k_dist(now_index) > new_total_dist || K * 2 < k) {
                                new_k_center(now_index) = updateVector(new_k_center(now_index), now_max_index, 0.5 * now_max)
                                new_k_center += updateVector(new_k_center(now_index), now_max_index, -0.5 * now_max)
                                new_k_n += 0
                                new_k_dist += 0

                                K += 1
                            }
                        }
                    }
                } else {
                    // 合并操作
                    val length = new_k_center.length
                    var mergeSet = Set[Array[Double]]()

                    // 判断哪些聚类中心需要合并
                    for (i <- 0 until length) {
                        for (j <- i + 1 until length) {
                            val dist = euclidDist(new_k_center(i), new_k_center(j))
                            if (dist < Oc) {
                                mergeSet += Array[Double](i, j, dist)
                            }
                        }
                    }

                    // 合并具体操作
                    if (mergeSet.nonEmpty) {
                        val sortList = mergeSet.toList.sortBy(data => data(2))
                        val flag: Array[Int] = new Array[Int](K)
                        var mergePointSet = Set[Int]()

                        for (item <- sortList) {
                            if (flag(item(0).toInt) == 0 && flag(item(1).toInt) == 0) {
                                val index1 = item(0).toInt
                                val index2 = item(1).toInt
                                flag(index1) = 1
                                flag(index2) = 1

                                val temp = addVector(mulVector(new_k_center(index1), new_k_n(index1)), mulVector(new_k_center(index2), new_k_n(index2)))
                                new_k_center(index1) = divVector(temp, new_k_n(index1) + new_k_n(index2))

                                mergePointSet += index2
                                K -= 1
                            }
                        }
                        val mergeList = mergePointSet.toList.sorted.reverse
                        for (item <- mergeList) {
                            new_k_center.remove(item)
                            new_k_n.remove(item)
                            new_k_dist.remove(item)
                        }
                    }
                }
                iter += 1
            }
        )
        time2 = System.currentTimeMillis()
        endtime = System.currentTimeMillis()

        println()
        println("the isodata time:" + (time2 - time1))

        time1 = System.currentTimeMillis()
        new_center_broad = spark.sparkContext.broadcast(new_k_center)
        image_rdd.mapPartitions(point => finalResult(point, new_center_broad)).saveAsTextFile(HADOOP_HOST + OUTPUT_PATH)
        time2 = System.currentTimeMillis()

        image_rdd.unpersist()

        println("the run time:" + (endtime - begintime))

        println(new_k_center.length)
        for (i <- new_k_center.indices) {
            new_k_center(i).foreach(item => print(item + " "))
            println()
        }

        spark.stop()
    }

    def finalResult(iter: Iterator[Array[Array[Double]]], broadcast: Broadcast[ArrayBuffer[Array[Double]]]) = {
        val centers = broadcast.value
        val res = new ArrayBuffer[(Int, Int, Int)]
        while (iter.hasNext) {
            val point = iter.next()
            var bestIndex = 0
            var closest = Double.MaxValue
            for (i <- centers.indices) {
                val tempDist = euclidDist(point(0), centers(i)) //欧氏距离
                if (tempDist < closest) {
                    closest = tempDist
                    bestIndex = i
                }
            }
            val te = (bestIndex, point(1)(0).toInt, point(1)(1).toInt)
            res += te
        }
        res.iterator
    }


    def centerDist(part: Iterator[(Int, (Array[Double], Int))], broadcast: Broadcast[ArrayBuffer[Array[Double]]]): Iterator[(Int, (Array[Double], Double))] = {
        val res = new ArrayBuffer[(Int, (Array[Double], Double))]
        val centers = broadcast.value
        for (index <- centers.indices) {
            var temp = (index, (Array[Double](0, 0), 0.0))
            res += temp
        }
        while (part.hasNext) {
            val point = part.next()
            val nowCenter = centers(point._1)

            val dist: Double = euclidDist(nowCenter, point._2._1)
            val vectorDist: Array[Double] = vectorStandardDeviation(point._2._1, nowCenter)

            val test = (point._1, (addVector(res(point._1)._2._1, vectorDist), res(point._1)._2._2 + dist))
            res(point._1) = test
        }
        res.iterator
    }

    def closestCenter(iter: Iterator[Array[Array[Double]]], broadcast: Broadcast[ArrayBuffer[Array[Double]]]): Iterator[(Int, (Array[Double], Int))] = {
        val centers = broadcast.value
        var res = new ArrayBuffer[(Int, (Array[Double], Int))]
        for (index <- centers.indices) {
            var temp = (index, (Array[Double](0, 0), 0))
            res += temp
        }
        while (iter.hasNext) {
            val point = iter.next()

            var bestIndex = 0
            var closest = Double.MaxValue
            for (i <- centers.indices) {
                val tempDist = euclidDist(point(0), centers(i)) //欧氏距离
                if (tempDist < closest) {
                    closest = tempDist
                    bestIndex = i
                }
            }
            val te = (bestIndex, (addVector(res(bestIndex)._2._1, point(0)), res(bestIndex)._2._2 + 1))
            res(bestIndex) = te
        }
        res.iterator
    }

    def initOn(width: Int, height: Int): Unit = {
        On = Math.sqrt(width * height).toInt
    }

    /*
    0: 分裂  1：合并
     */
    def judgeOperation(iter: Int): Int = {
        if (K * 2 <= k) {
            0
        } else if (K >= k * 2) {
            1
        } else {
            if (iter % 2 == 0) {
                1
            } else {
                0
            }
        }
    }

    def vectorStandardDeviation(arr1: Array[Double], arr2: Array[Double]): Array[Double] = {
        val arr = new Array[Double](arr1.length)
        for (i <- arr1.indices) {
            arr(i) = Math.pow(arr1(i) - arr2(i), 2)
        }
        arr
    }

    def euclidDist(point1: Array[Double], point2: Array[Double]): Double = {
        var dist: Double = 0
        for (i <- point1.indices) {
            dist += Math.pow(point1(i) - point2(i), 2)
        }
        Math.sqrt(dist)
    }

    def updateVector(arr: Array[Double], index: Int, data: Double): Array[Double] = {
        val arr1 = arr.clone()
        arr1(index) += data
        arr1
    }

    def sqrtDivVector(arr: Array[Double], division: Int): Array[Double] = {
        val arr1 = arr.clone()
        for (i <- arr1.indices) {
            arr1(i) = Math.sqrt(arr1(i) / division)
        }
        arr1
    }

    def addVector(arr1: Array[Double], arr2: Array[Double]): Array[Double] = {
        val arr = new Array[Double](arr1.length)
        for (i <- arr1.indices) {
            arr(i) = arr1(i) + arr2(i)
        }
        arr
    }

    def divVector(arr: Array[Double], division: Int): Array[Double] = {
        val arr1 = arr.clone()
        for (i <- arr1.indices) {
            arr1(i) = arr1(i) / division
        }
        arr1
    }

    def mulVector(arr: Array[Double], multiply: Int): Array[Double] = {
        val arr1 = arr.clone()
        for (i <- arr1.indices) {
            arr1(i) = arr1(i) * multiply
        }
        arr1
    }

}
