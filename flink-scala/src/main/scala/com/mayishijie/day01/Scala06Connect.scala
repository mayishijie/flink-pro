package com.mayishijie.day01

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object Scala06Connect {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env.fromElements(("tty", 18), ("wjj", 16))
    val stream2 = env.fromElements(("tty", 21), ("wjj", 22))

    val connectStream = stream1.keyBy(_._1).connect(stream2.keyBy(_._1))

    connectStream.map(new CoMapFunction[(String,Int),(String,Int),String] {
      //处理来自第一条流的元素
      override def map1(value: (String, Int)): String = {
        value._1+"====="+value._2
      }

      //处理来自第二条流的元素
      override def map2(value: (String, Int)): String = {
        value._1+"**********"+value._2
      }
    }).print()

    env.execute("connectJob")
  }
}
