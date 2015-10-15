package com.donson.devicelog

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ch on 10/15/15.
 */
object DeviceLogdraw {

  /**
   * 从平台日志中抽取设备相关的数据，作为用户上网行为分析的数据源
   * |-- Device      string   	设备型号，如HTC、iPhone'
      -- Client      tinyint	  设备类型
      -- OsVersion   string   	设备操作系统版本，如4.0'
      -- Density     string   	设备屏幕的密度
      -- Pw      int   	设备屏幕宽度'
      -- Ph      int   	设备屏幕高度'
   * @param args
   */
  def main(args: Array[String]) {
    if (args.size < 2) {
      println("Usage: DeviceLogdraw <fileSource> <resultDir>")
      System.exit(0)
    }

    val Array(fileSource, resultDir) = args

    val sparkConf = new SparkConf().setAppName("DeviceLogdraw")
    val sc = new SparkContext(sparkConf)

    val deviceData = sc.textFile(fileSource).map(_.trim.split(",")).map({
      t => {
        t(16).concat(",").concat(t(17))
          .concat(",").concat(t(18))
          .concat(",").concat(t(19))
          .concat(",").concat(t(20))
          .concat(",").concat(t(21))
      }
    })

    deviceData.saveAsTextFile(resultDir)
    sc.stop()
  }

}
