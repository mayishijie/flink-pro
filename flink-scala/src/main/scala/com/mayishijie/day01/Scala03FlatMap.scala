package com.mayishijie.day01

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

object Scala03FlatMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new SensorSource)

    //flatMap可以实现map和filter的功能,既可以过滤,也可以做数据转换
    stream.flatMap(new FlatMapFunction[SensorReading, SensorReading] {
      override def flatMap(value: SensorReading, out: Collector[SensorReading]): Unit = {
        if(value.id=="sensor_1") {
          //out.collect直接将数据发送到下游,可以每调用一次out.collect,则会发送一次到下游,所以多执行几次,就可以多发送重复数据到下游
          out.collect(value)
        }
      }
    }).print()
    env.execute("flatMapJob")
  }
}
