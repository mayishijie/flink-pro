package com.mayishijie.day01

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment,createTypeInformation}

object Scala05Union {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val JX:DataStream[SensorReading] = env.addSource(new SensorSource).filter(_.id.equals("sensor_1"))
    val AW:DataStream[SensorReading] = env.addSource(new SensorSource).filter(_.id.equals("sensor_2"))
    val HN:DataStream[SensorReading] = env.addSource(new SensorSource).filter(_.id.equals("sensor_3"))

    val stream = JX.union(AW, HN).print()

    env.execute("unionJob")
  }
}
