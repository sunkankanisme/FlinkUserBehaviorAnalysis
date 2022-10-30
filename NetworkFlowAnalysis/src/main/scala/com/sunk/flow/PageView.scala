package com.sunk.flow

import org.apache.flink.api.common.functions.{AggregateFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/*
 * pv: PageView 页面访问量
 */
object PageView {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 从文件中读取数据
        val resource = getClass.getResource("/UserBehavior.csv")
        print(resource)
        val inputStream = env.readTextFile(resource.getPath)

        // 转换成样例类，并提取时间戳，生成 WaterMark（此数据源是有序的所以不指定 WaterMark）
        val dataStream: DataStream[UserBehavior] = inputStream.map(line => {
            val arr = line.split(",")
            UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
        }).assignAscendingTimestamps(_.timestamp * 1000)

        // 使用 "pv" 作为一个哑 key, 做 keyBy 之后所有的数据会被分入一个组中
        // 或者使用随机数进行打散, new RandomMapper(10)，然后再使用窗口聚合函数进行聚合
        val pvStream: DataStream[PvCount] = dataStream
                .filter(_.behavior == "pv")
                // .map(_ => ("pv", 1L))
                .map(new RandomMapper(10))
                .keyBy(_._1)
                .timeWindow(Time.hours(1))
                .aggregate(new PvCountAgg(), new PvCountWindowResult())

        // 聚合所有分区的数据
        val totalPvStream = pvStream.keyBy(_.windowEnd).process(new TotalProcessFunction())

        totalPvStream.print()
        env.execute()
    }


    // 定义输入数据样例类
    case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

    // 定义中间输出结果样例类
    case class PvCount(windowEnd: Long, count: Long)

    // 自定义预聚合函数
    class PvCountAgg() extends AggregateFunction[(Long, Long), Long, Long] {
        override def createAccumulator(): Long = 0L

        override def add(in: (Long, Long), acc: Long): Long = acc + 1

        override def getResult(acc: Long): Long = acc

        override def merge(acc: Long, acc1: Long): Long = acc + acc1
    }

    // 自定义窗口聚合函数
    class PvCountWindowResult() extends WindowFunction[Long, PvCount, Long, TimeWindow] {
        override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
            out.collect(PvCount(window.getEnd, input.head))
        }
    }

    class RandomMapper(i: Int) extends RichMapFunction[UserBehavior, (Long, Long)] {
        override def map(in: UserBehavior): (Long, Long) = {
            (Random.nextInt(i), 1)
        }
    }

    class TotalProcessFunction() extends KeyedProcessFunction[Long, PvCount, PvCount] {
        lazy val totalPvCount: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total", classOf[Long]))

        override def processElement(i: PvCount, context: KeyedProcessFunction[Long, PvCount, PvCount]#Context, collector: Collector[PvCount]): Unit = {
            println("=== " + context.getCurrentKey + " " + i)
            // 每来一条数据就将 total 更新，累加上去
            totalPvCount.update(totalPvCount.value() + i.count)

            // 注册定时器
            context.timerService().registerEventTimeTimer(i.windowEnd + 1)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
            out.collect(PvCount(ctx.getCurrentKey, totalPvCount.value()))

            // 清空状态
            totalPvCount.clear()
        }
    }
}
