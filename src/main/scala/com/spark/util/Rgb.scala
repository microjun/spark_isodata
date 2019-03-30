package com.spark.util

/**
  * Created by IntelliJ IDEA.
  * User: Kekeer
  * Date: 2019/3/29
  * Time: 14:12
  */
object Rgb {
    def Rgb2Lab(R: Double, G: Double, B: Double): Array[Double] = {
        val lab: Array[Double] = new Array[Double](3)
        var l: Double = .0
        var a: Double = .0
        var b: Double = .0
        var x: Double = .0
        var y: Double = .0
        var z: Double = .0
        var fx: Double = .0
        var fy: Double = .0
        var fz: Double = .0
        val BLACK: Double = 20.0
        val YELLOW: Double = 70.0

        //转化为xyz
        x = 0.412453 * R + 0.357580 * G + 0.180423 * B
        y = 0.212671 * R + 0.715160 * G + 0.072169 * B
        z = 0.019334 * R + 0.119193 * G + 0.950227 * B

        // 归一化处理
        x = x / (255.0 * 0.950456)
        y = y / 255.0
        z = z / (255.0 * 1.088754)
        if (y > 0.008856) {
            fy = Math.pow(y, 1.0 / 3.0)
            l = 116.0 * fy - 16.0
        }
        else {
            fy = 7.787 * y + 16.0 / 116.0
            l = 903.3 * y
        }
        if (x > 0.008856) {
            fx = Math.pow(x, 1.0 / 3.0)
        }
        else {
            fx = 7.787 * x + 16.0 / 116.0
        }
        if (z > 0.008856) {
            fz = Math.pow(z, 1.0 / 3.0)
        }
        else {
            fz = 7.787 * z + 16.0 / 116.0
        }
        a = 500.0 * (fx - fy)
        b = 200.0 * (fy - fz)
        if (l < BLACK) {
            a *= Math.exp((l - BLACK) / (BLACK / 4))
            b *= Math.exp((l - BLACK) / (BLACK / 4))
            l = 20
        }
        if (b > YELLOW) b = YELLOW
        lab(0) = l
        lab(1) = a
        lab(2) = b
        lab
    }

    def Lab2Rgb(lab: Array[Double]): Array[Double] = {
        val rgb: Array[Double] = new Array[Double](3)
        val l: Double = lab(0)
        val a: Double = lab(1)
        val b: Double = lab(2)
        var fx: Double = .0
        var fy: Double = .0
        var fz: Double = .0
        var x: Double = .0
        var y: Double = 0
        var z: Double = .0
        var dr: Double = .0
        var dg: Double = .0
        var db: Double = .0
        fy = (l + 16.0) / 116.0
        fy = fy * fy * fy
        if (fy > 0.008856) {
            y = fy
        }
        else {
            fy = l / 903.3
        }
        if (fy > 0.008856) {
            fy = Math.pow(fy, 1.0 / 3.0)
        }
        else {
            fy = 7.787 * fy + 16.0 / 116.0
        }
        fx = a / 500.0 + fy
        if (fx > 0.206893) {
            x = Math.pow(fx, 3.0)
        }
        else {
            x = (fx - 16.0 / 116.0) / 7.787
        }
        fz = fy - b / 200.0
        if (fz > 0.206893) {
            z = Math.pow(fz, 3)
        }
        else {
            z = (fz - 16.0 / 116.0) / 7.787
        }
        x = x * 0.950456 * 255.0
        y = y * 255.0
        z = z * 1.088754 * 255.0
        dr = 3.240479 * x - 1.537150 * y - 0.498535 * z
        dg = -0.969256 * x + 1.875992 * y + 0.041556 * z
        db = 0.055648 * x - 0.204043 * y + 1.057311 * z
        if (dr < 0) {
            dr = 0
        }
        if (dr > 255) {
            dr = 255
        }
        if (dg < 0) {
            dg = 0
        }
        if (dg > 255) {
            dg = 255
        }
        if (db < 0) {
            db = 0
        }
        if (db > 255) {
            db = 255
        }
        rgb(0) = dr
        rgb(1) = dg
        rgb(2) = db
        rgb
    }
}
