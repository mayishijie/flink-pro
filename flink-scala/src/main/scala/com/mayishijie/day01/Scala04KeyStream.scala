package com.mayishijie.day01

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object Scala04KeyStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new SensorSource).filter(_.id.equals("sensor_1"))

    stream.keyBy(_.id).min(2).print()

    env.execute("keyStreamJob")
  }
}
