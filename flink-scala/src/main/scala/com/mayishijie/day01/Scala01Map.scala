package com.mayishijie.day01

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object Scala01Map {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new SensorSource())

    //1. 直接通过map方法,不过这种方法相对来说,会局限一点
    //stream.map(r=>r.id).print()

    //2. 实现接口方式
    stream.map(new MyMap()).print()

    // 3. 直接通过匿名类
    //stream.map(new MapFunction[SensorReading,String] {
    //  override def map(value: SensorReading): String = value.id
    //})

    env.execute("mapJob")
  }

  //class MyMap extends MapFunction[SensorReading]{
  //
  //}
}

//map算子转换,因为存在输入和输出类型不一致情况,所以是2种类型,输入和输出类型
class MyMap extends MapFunction[SensorReading,String]{
  override def map(value: SensorReading): String = {
    value.id
  }

}
