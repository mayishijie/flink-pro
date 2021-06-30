package com.mayishijie.day02

import com.mayishijie.day01.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Window02AavgTempFunction {

  case class AvgInfo(key:String,avg:Long,startTime:Long,endTime:Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new SensorSource)
    stream.keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .process(new MyProcess)
      .print()

    env.execute("avgTemp")
  }

  class MyProcess extends ProcessWindowFunction[SensorReading,AvgInfo,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[AvgInfo]): Unit = {
      val size = elements.size
      var num = 0L
      for (elem <- elements) {
        num += elem.temperature
      }
      out.collect(AvgInfo(key,num/size,context.window.getStart,context.window.getEnd))
    }
  }
}
