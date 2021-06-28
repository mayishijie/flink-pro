package com.mayishijie.day01

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object Scala02Filter {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new SensorSource)

    //stream.filter(r=>r.temperature>20).print()

    //filter算子输入和输出的数据类型是一致的,所有只需要一个类型
    stream.filter(new FilterFunction[SensorReading] {
      override def filter(value: SensorReading): Boolean = value.timestamp > 20
    }).print()

    env.execute("filterJob")
  }
}
