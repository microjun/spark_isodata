package com.spark

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.File
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import com.spark.statistics.Oc_2.{HADOOP_HOST, K, getRgb}
import com.spark.util.Rgb
import javax.imageio.ImageIO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
  * Created by IntelliJ IDEA.
  * User: Kekeer
  * Date: 2019/3/27
  * Time: 14:23
  */
object K_means_4_19 {

//    val HADOOP_HOST: String = "hdfs://" + "219.245.196.115" + ":9000"
//    val MASTER: String = "spark://219.245.196.115:7077"
//    val INPUT_PATH: String = "/keke/input/data0.jpg"
//    val OUTPUT_PATH: String = "/keke/output/"

        val HADOOP_HOST: String = ""
        val MASTER: String = "local"
        val INPUT_PATH: String = "input/data.jpg"
        val OUTPUT_PATH: String = "output/"

    var K: Int = 10 // 初始聚类数
    val k: Int = 4 // 期望得到的聚类数
    var On: Int = 100 // 每个类别中最小样本数，小于这个值，则取消
    val Oc: Double = 20 // 两个类别间最小距离，小于这个值，则合并
    val Os: Double = 10000 // 一个类别中样本特征中最大标准差，大于这个值，则分裂
    val I: Int = 20 // 迭代运行的最大次数

    var new_k_center: ArrayBuffer[Vector] = new ArrayBuffer[Vector](K) // 各聚类中心
    var new_k_n: ArrayBuffer[Int] = new ArrayBuffer[Int](K) // 每个聚类对应的样本个数
    var new_k_dist: ArrayBuffer[Double] = new ArrayBuffer[Double](K) // 各聚类域中模式样本与各聚类中心间的平均距离
    var new_total_dist: Double = 0.0 // 样本和其对应聚类中心的总平均距离

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
            .appName("K_means")
            .master(MASTER)
//            .config("spark.executor.memory", "4g")
            //            .config("spark.default.parallelism", "100")
            .getOrCreate

        val image_df = spark.read.format("image").load(HADOOP_HOST + INPUT_PATH)
        val height: Int = image_df.select("image.height").first().get(0).toString.toInt
        val width: Int = image_df.select("image.width").first().get(0).toString.toInt

        val old_time = System.currentTimeMillis()
        val data: Array[Byte] = image_df.select("image.data").first().getAs[Array[Byte]](0)
        val lab_vec: Array[Vector] = getLabVec(width, height, getLabArr(width, height, data))
        val now_time = System.currentTimeMillis()
        println("the time :" + (now_time - old_time))

        val image_rdd = spark.sparkContext.parallelize(lab_vec)

        new_k_center ++= image_rdd.takeSample(true, K)
        init()
        initOn(width, height)
        println("On:" + On)

        var iter: Int = 1
        var dist: Double = 1.0
        var cancel_flag: Int = 0

        breakable(
            while (true) {
                println()
                println(nowDate())
                //                println("the " + iter + " times: ")
                //                println("the dist is: " + dist)
                println("the cancel flag: " + cancel_flag)
                println("the now center: " + new_k_center.size)

                if (dist < 0.01 || iter > I) {
                    break()
                }

                // 判断是否需要重新随机取聚类中心
                if (cancel_flag == 1) {
                    new_k_center.clear()
                    new_k_center ++= image_rdd.takeSample(true, K)
                    init()
                    cancel_flag = 0
                }

                // 根据聚类中心进行聚类
                val closest_rdd = image_rdd.map(
                    iter => (closestCenter(iter, new_k_center), (iter, 1))
                ).groupByKey().persist()
                val pointStats = closest_rdd.map(
                    iter => (iter._1, iter._2.reduce(
                        (x1, x2) => (addVector(x1._1, x2._1), x1._2 + x2._2)
                    ))
                )

                // 判断是否有类别中最小样本数小于最小样本数On，若存在则continue，重新迭代
                val newPoint = pointStats.collect()
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
                dist = 0.0
                for (item <- newPoint) {
                    val nowVector = item._2._1
                    val nowN = item._2._2
                    dist += Math.sqrt(Vectors.sqdist(divVector(nowVector, nowN), new_k_center(item._1)))
                    new_k_center(item._1) = nowVector
                    new_k_n(item._1) = nowN
                }

                // 计算各聚类域中模式样本与各聚类中心间的总欧式距离
                val o_dist_rdd = closest_rdd.map(iter => (iter._1, iter._2.map(
                    item => oVectorDist(item._1, new_k_center(iter._1))
                ))).map(iter => (iter._1, iter._2.sum)).collect()

                // 计算各聚类域中模式样本与各聚类中心间的平均欧式距离
                var total_o_dist = 0.0
                for (iter <- o_dist_rdd) {
                    total_o_dist += iter._2
                    new_k_dist(iter._1) = iter._2 / new_k_n(iter._1)
                }
                new_total_dist = total_o_dist / (width * height)

                val operation: Int = judgeOperation(iter)
                if (operation == 0) {
                    // 分裂操作
                    println("this is split")
                    val split_rdd = closest_rdd.map(iter => (iter._1, iter._2.map(item => (
                        vectorStandardDeviation(item._1, new_k_center(iter._1)), item._2
                    )).reduce((x1, x2) => (
                        addVector(x1._1, x2._1), x1._2 + x2._2
                    ))))

                    val split_arr = split_rdd.map(iter => (iter._1, sqrtDivVector(iter._2._1, iter._2._2))).collect()

                    for (i <- split_arr.indices) {
                        val now_index: Int = split_arr(i)._1
                        val now_vec_arr: Array[Double] = split_arr(i)._2.toArray
                        var now_max: Double = Double.MinValue
                        var now_max_index: Int = 0
                        for (j <- now_vec_arr.indices) {
                            if (now_vec_arr(j) > now_max) {
                                now_max = now_vec_arr(j)
                                now_max_index = j
                            }
                        }

                        if (now_max > Os) {
                            if (new_k_dist(now_index) > new_total_dist || K < k / 2) {
                                print("split success")
                                new_k_center(now_index) = updateVector(new_k_center(now_index), now_max_index, 0.5 * now_max)
                                new_k_center += updateVector(new_k_center(now_index), now_max_index, -0.5 * now_max)
                                new_k_n += 0
                                new_k_dist += 0

                                K += 1
                            }
                        }
                    }

                } else {
                    println("this is merge")
                    // 合并操作
                    val length = new_k_center.length
                    var mergeSet = Set[Array[Double]]()

                    // 判断哪些聚类中心需要合并
                    for (i <- 0 until length) {
                        for (j <- i + 1 until length) {
                            val dist = oVectorDist(new_k_center(i), new_k_center(j))
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
                                println("merge success")
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

        // 输出分割结果
        val index = image_rdd.map(iter => closestCenter(iter, new_k_center)).collect()
        writeImage(width, height, index, data)

        spark.stop()

    }

    def nowDate(): String = {
        val now: Date = new Date()
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val date = dateFormat.format(now)
        date
    }

    def init(): Unit = {
        new_k_n.clear()
        new_k_n ++= new Array[Int](K)
        new_k_dist.clear()
        new_k_dist ++= new Array[Double](K)
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

    def writeImage(width: Int, height: Int, index: Array[Int], data: Array[Byte]): Unit = {
        var iter: Int = 0
        val bim = new Array[BufferedImage](K)
        for (i <- bim.indices) {
            bim(i) = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
        }
        for (h <- 0 until height) {
            for (w <- 0 until width) {
                bim(index(width * h + w)).setRGB(w, h, getRgb(data(iter + 2), data(iter + 1), data(iter)))
                iter = iter + 3
            }
        }
        for (i <- bim.indices) {
            //            ImageIO.write(bim(i), "jpg", new File(HADOOP_HOST+OUTPUT_PATH))
            val path: String = HADOOP_HOST + OUTPUT_PATH + "result" + i + ".jpg"
            val fs = FileSystem.get(URI.create(path), new Configuration)
            val out: FSDataOutputStream = fs.create(new Path(path))

            ImageIO.write(bim(i), "jpg", out)

            out.close()
        }
    }

    def vectorStandardDeviation(vector1: Vector, vector2: Vector): Vector = {
        val arr1 = vector1.toArray
        val arr2 = vector2.toArray
        for (i <- arr1.indices) {
            arr1(i) = Math.pow(arr1(i) - arr2(i), 2)
        }
        Vectors.dense(arr1)
    }

    def oVectorDist(vector1: Vector, vector2: Vector): Double = {
        val arr1 = vector1.toArray
        val arr2 = vector2.toArray
        var o_dist: Double = 0.0
        for (i <- arr1.indices) {
            o_dist += Math.pow(arr1(i) - arr2(i), 2)
        }
        Math.sqrt(o_dist)
    }

    def updateVector(vector1: Vector, index: Int, data: Double): Vector = {
        val arr1 = vector1.toArray
        arr1(index) = data
        Vectors.dense(arr1)
    }

    def addVector(vector1: Vector, vector2: Vector): Vector = {
        val arr1 = vector1.toArray
        val arr2 = vector2.toArray
        val arr = new Array[Double](arr1.length)
        for (i <- arr1.indices) {
            arr(i) = arr1(i) + arr2(i)
        }
        Vectors.dense(arr)
    }

    def sqrtDivVector(vector1: Vector, division: Int): Vector = {
        val arr1 = vector1.toArray
        for (i <- arr1.indices) {
            arr1(i) = Math.sqrt(arr1(i) / division)
        }
        Vectors.dense(arr1)
    }

    def divVector(vector1: Vector, division: Int): Vector = {
        val arr1 = vector1.toArray
        for (i <- arr1.indices) {
            arr1(i) = arr1(i) / division
        }
        Vectors.dense(arr1)
    }

    def mulVector(vector1: Vector, multiply: Int): Vector = {
        val arr1 = vector1.toArray
        for (i <- arr1.indices) {
            arr1(i) = arr1(i) * multiply
        }
        Vectors.dense(arr1)
    }

    def closestCenter(point: Vector, centers: ArrayBuffer[Vector]): Int = {
        var bestIndex = 0
        var closest = Double.PositiveInfinity
        for (i <- centers.indices) {
            val tempDist = Math.sqrt(Vectors.sqdist(point, centers(i))) //欧氏距离
            if (tempDist < closest) {
                closest = tempDist
                bestIndex = i
            }
        }
        bestIndex
    }

    def getLabVec(width: Int, height: Int, lab_arr: Array[Array[Double]]): Array[Vector] = {
        val lab_vec: Array[Vector] = new Array[Vector](height * width)
        var iter: Int = 0
        for (h <- 0 until height) {
            for (w <- 0 until width) {
                lab_vec(width * h + w) = Vectors.dense(lab_arr(width * h + w)(1), lab_arr(width * h + w)(2))
                iter = iter + 3
            }
        }
        lab_vec
    }

    def getLabArr(width: Int, height: Int, data: Array[Byte]): Array[Array[Double]] = {
        val lab_arr: Array[Array[Double]] = new Array[Array[Double]](height * width)
        var iter: Int = 0
        for (h <- 0 until height) {
            for (w <- 0 until width) {
                lab_arr(width * h + w) = Rgb.Rgb2Lab(byteToInt(data(iter + 2)), byteToInt(data(iter + 1)), byteToInt(data(iter)))
                iter = iter + 3
            }
        }
        lab_arr
    }

    // 将字节数组rgb写入文件
    def writeImageByByte(width: Int, height: Int, data: Array[Byte]): Unit = {
        val image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
        var iter: Int = 0
        for (h <- 0 until height) {
            for (w <- 0 until width) {
                image.setRGB(w, h, getRgb(data(iter + 2), data(iter + 1), data(iter)))
                iter = iter + 3
            }
        }

        ImageIO.write(image, "jpg", new File("output.jpg"))
    }


    def writeImageByLabDouble(width: Int, height: Int, lab_arr: Array[Array[Double]]): Unit = {

        val rgb_arr: Array[Array[Double]] = new Array[Array[Double]](height * width)
        for (h <- 0 until height) {
            for (w <- 0 until width) {
                rgb_arr(width * h + w) = Rgb.Lab2Rgb(lab_arr(h * width + w))
            }
        }

        val image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
        for (h <- 0 until height) {
            for (w <- 0 until width) {
                image.setRGB(w, h, getRgbByDouble(rgb_arr(h * width + w)(0), rgb_arr(h * width + w)(1), rgb_arr(h * width + w)(2)))
            }
        }

        ImageIO.write(image, "jpg", new File("output.jpg"))
    }

    def byteToInt(data: Byte): Int = {
        data & 0xff
    }

    // 得到rgb值
    def getRgb(r: Byte, g: Byte, b: Byte): Int = {
        new Color(byteToInt(r), byteToInt(g), byteToInt(b)).getRGB
    }

    def getRgbByDouble(r: Double, g: Double, b: Double): Int = {
        new Color(r.toInt, g.toInt, b.toInt).getRGB
    }
}
