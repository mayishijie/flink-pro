package com.mayishijie.day01

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Scala07CoFlatMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.execute("coFlatMapJob")
  }
}
