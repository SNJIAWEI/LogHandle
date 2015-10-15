package com.donson.applog

import com.donson.common.Utils
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ll on 10/15/15.
 */
object AppLogdraw {

  /**
   * 从平台日志中抽取app相关的数据，作为用户应用偏好分析的数据源
   * @param args
   */
  def main(args: Array[String]) {

    if (args.size < 4) {
      println("Usage: AppLogdraw <fileSource> <hanZiDir> <numberDir> <otherDir>")
      System.exit(0)
    }

    val Array(fileSource, hanZiDir, numberDir, otherDir) = args

    val sparkConf = new SparkConf().setAppName("AppLogdraw")
    val sc = new SparkContext(sparkConf)

    val appData = sc.textFile(fileSource)
    val lineData = appData.map(_.split(","))

    /**
     * appid为汉字
     */
    lineData.filter(a => (a.length > 55 && (Utils.isContainCH((a(13)))))).map(a => {
      a(12) + "," + a(13) + "," + a(14) + "," + a(15) + "," + a(22) + "," + a(23) + "," + a(24) + "," + a(25) + "," + a(46) + "," + a(47) + "," + a(48) + "," + a(49) + "," + a(50) + "," + a(55)
    }).saveAsTextFile(hanZiDir)

    /**
     * appid为数字
     */
    lineData.filter(a => (a.length > 55 && (Utils.isAllNumber((a(13)))))).map(a => {
      a(12) + "," + a(13) + "," + a(14) + "," + a(15) + "," + a(22) + "," + a(23) + "," + a(24) + "," + a(25) + "," + a(46) + "," + a(47) + "," + a(48) + "," + a(49) + "," + a(50) + "," + a(55)
    }).saveAsTextFile(numberDir)

    /**
     * appid非汉字费数字
     */
    lineData.filter(a => (a.length > 55 && !(Utils.isContainCH((a(13)))) && !(Utils.isAllNumber((a(13)))))).map(a => {
      a(12) + "," + a(13) + "," + a(14) + "," + a(15) + "," + a(22) + "," + a(23) + "," + a(24) + "," + a(25) + "," + a(46) + "," + a(47) + "," + a(48) + "," + a(49) + "," + a(50) + "," + a(55)
    }).saveAsTextFile(otherDir)

    sc.stop()
  }

}
