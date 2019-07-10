package com.spark

import java.awt.image.BufferedImage
import java.io.{BufferedReader, File, FileOutputStream, FileReader}
import javax.imageio.ImageIO

/**
  * Created by root on 19-5-21.
  */
object TextToImage {


  val result_path:String = "/home/lb/Test1/spark_result"

  def main(args: Array[String]) {
    val time1 = System.currentTimeMillis()
    textToImage("input/1006.jpg",result_path+"/text",2)
    val time2 = System.currentTimeMillis()
    println("the write time:"+(time2-time1))
  }

  def textToImage(localPath:String,path:String,K:Int):Unit ={


    val bi: BufferedImage = ImageIO.read(new File(localPath))
    val width: Int = bi.getWidth
    val height: Int = bi.getHeight
    val bim = new Array[BufferedImage](K)

    for (i <- bim.indices) {
      bim(i) = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
    }

    val dir = new File(path)
    val fileList = dir.listFiles()

    var i:Int = 0
    for(file <- fileList) {
      if(file.getName.contains("part")) {
        println(file.getName)
        val br: BufferedReader = new BufferedReader(new FileReader(file))

        var str:String = null
        do {
          str = br.readLine()
          if (str != null) {
            i += 1
            val strings = str.replace("(", "").replace(")", "").split(",")
            val centerid: Integer = Integer.parseInt(strings(0))
            bim(centerid).setRGB(Integer.parseInt(strings(2)), Integer.parseInt(strings(1)), bi.getRGB(Integer.parseInt(strings(2)), Integer.parseInt(strings(1))))

          }

        } while (str != null)
      }
    }
    println(i)

    for (i <- bim.indices) {
      //            ImageIO.write(bim(i), "jpg", new File(HADOOP_HOST+OUTPUT_PATH))

      ImageIO.write(bim(i), "jpg", new FileOutputStream(result_path+"/1002" +
        "_" + i + ".jpg"))

    }

  }

}
