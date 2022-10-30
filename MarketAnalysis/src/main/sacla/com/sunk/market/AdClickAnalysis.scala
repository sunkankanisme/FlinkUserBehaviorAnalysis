package com.sunk.market

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
 * 广告点击量相关统计
 */
object AdClickAnalysis {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 读取数据
        val resource = getClass.getResource("/AdClickLog.csv")
        val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

        // 转换成样例类并提取时间戳和设置 WaterMark
        val adLogStream = inputStream.map(data => {
            val arr = data.split(",")
            AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
        }).assignAscendingTimestamps(_.timestamp * 1000)

        // 开窗聚合统计
        val resultStream = adLogStream.keyBy(_.province)
                .timeWindow(Time.days(1), Time.seconds(5))
                .aggregate(new AdCountAgg(), new AdCountWindowResult())
        resultStream.print()

        env.execute()
    }

    // 定义输入输出样例类
    case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

    case class AdClickCountByProvince(windowStart: String, windowEnd: String, province: String, count: Long)

    class AdCountAgg() extends AggregateFunction[AdClickLog, Long, Long] {
        override def createAccumulator(): Long = 0L

        override def add(in: AdClickLog, acc: Long): Long = acc + 1

        override def getResult(acc: Long): Long = acc

        override def merge(acc: Long, acc1: Long): Long = acc + acc1
    }

    class AdCountWindowResult() extends WindowFunction[Long, AdClickCountByProvince, String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdClickCountByProvince]): Unit = {
            out.collect(AdClickCountByProvince(
                new Timestamp(window.getStart).toString,
                new Timestamp(window.getEnd).toString,
                key, input.head
            ))
        }
    }
}
