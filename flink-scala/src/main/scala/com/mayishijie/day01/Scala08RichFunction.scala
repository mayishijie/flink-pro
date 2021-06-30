package com.mayishijie.day01

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object Scala08RichFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements("scala spark flink is flink is best")

    stream.map(new RichMapFunction[String,String] {
      override def open(parameters: Configuration): Unit = {
        println("program is start")
      }
      override def map(value: String): String = {
        val name = getRuntimeContext.getTaskName
        println("program is running:"+value+"==="+name)
        value+"==="+name
      }

      override def close(): Unit = {
        println("program is over")
      }
    }).print()

    env.execute("richFunction")
  }
}
