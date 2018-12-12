package com.microsoft.pnp

import org.apache.spark.listeners.LogAnalyticsStreamingListener
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object PiCalculator {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().getOrCreate()
    val sc = session.sparkContext
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.addStreamingListener(new LogAnalyticsStreamingListener(ssc.sparkContext.getConf))

    val slices = if (args.length < 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt
    val xs = 1 until n
    val rdd = sc.parallelize(xs, slices)
      .setName("'Initial rdd'")
    val sample = rdd.map { i => {
      val x = (math.random * 2) - 1
      val y = (math.random * 2) - 1
      (x, y)
    }
    }.setName("'Random points sample'")

    val inside = {
      sample.filter { case (x, y) => (x * x + y * y < 1) }.setName("'Random points inside circle'")
    }
    val count = inside.count()
    println("Pi is roughly " + 4.0 * count / n)

  }

}
