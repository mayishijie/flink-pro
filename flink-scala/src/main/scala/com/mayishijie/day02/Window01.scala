package com.mayishijie.day02

import com.mayishijie.day01.SensorSource
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

object Window01 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceStream = env.addSource(new SensorSource)

    sourceStream.keyBy(_.id).timeWindow(Time.seconds(10),Time.seconds(5))
  }
}
