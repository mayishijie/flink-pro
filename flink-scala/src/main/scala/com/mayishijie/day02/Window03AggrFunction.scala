package com.mayishijie.day02

import com.mayishijie.day01.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

object Window03AggrFunction {
  case class AccInfo(key:String,tempId:Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new SensorSource)
      .keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .aggregate(new AggregateFunction[SensorReading,(String,Long,Long),(String,Long)] {
        //创建一个空的累加器
        override def createAccumulator(): (String, Long, Long) = ("",0L,0L)

        //累加器中的逻辑
        override def add(value: SensorReading, accumulator: (String, Long, Long)): (String, Long, Long) = {
          (value.id,accumulator._2+1,accumulator._3+value.temperature)
        }

        //窗口闭合时,获取累加器的结果,值
        override def getResult(accumulator: (String, Long, Long)): (String, Long) = {
          (accumulator._1,accumulator._3/accumulator._2)
        }

        //2个累加器合并的结果
        override def merge(a: (String, Long, Long), b: (String, Long, Long)): (String, Long, Long) = {
          (a._1,a._2+b._2,a._3+b._3)
        }
      }).print()

    env.execute("aggrFunctionJob")
  }
}
