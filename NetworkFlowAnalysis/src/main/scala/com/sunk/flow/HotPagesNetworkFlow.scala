package com.sunk.flow

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object HotPagesNetworkFlow {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 加载数据，转换为样例类，提取时间戳生成 WaterMark
        val inputStream = env.readTextFile("NetworkFlowAnalysis/src/main/resources/apache.log")

        val middleStream: DataStream[ApacheEventLog] = inputStream.map(line => {
            val arr = line.split(" ")

            // 对事件时间进行转换得到时间戳
            val dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
            val ts = dateFormat.parse(arr(3)).getTime

            ApacheEventLog(arr(0), arr(1), ts, arr(5), arr(6))
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheEventLog](Time.minutes(3)) {
            override def extractTimestamp(t: ApacheEventLog): Long = t.timestamp
        })

        // 数据过滤
        val filterStream: DataStream[ApacheEventLog] = middleStream
                .filter(_.method == "GET")
                .filter(data => {
                    val pattern = "^((?!\\.(css|js|ico|png)$).)*$".r
                    (pattern findFirstIn data.url).nonEmpty
                })

        // 数据预聚合
        val aggStream = filterStream.keyBy(_.url)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(new OutputTag[ApacheEventLog]("late"))
                .aggregate(new PageCountAgg(), new PageViewCountWindowResult())

        // 进行开窗聚合，以及排序输出
        val resultStream = aggStream.keyBy(_.windowEnd).process(new TopNHotPages(3))

        // 输出
        resultStream.print("RES")
        aggStream.getSideOutput(new OutputTag[ApacheEventLog]("late")).print("LATE")

        env.execute()
    }

    // 输入数据样例类
    case class ApacheEventLog(ip: String, userId: String, timestamp: Long, method: String, url: String)

    // 窗口聚合结果样例类
    case class PageViewCount(url: String, windowEnd: Long, count: Long)

    class PageCountAgg() extends AggregateFunction[ApacheEventLog, Long, Long] {
        override def createAccumulator(): Long = 0L

        override def add(in: ApacheEventLog, acc: Long): Long = acc + 1

        override def getResult(acc: Long): Long = acc

        override def merge(acc: Long, acc1: Long): Long = acc + acc1
    }

    class PageViewCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
            out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))
        }
    }

    class TopNHotPages(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {
        // 使用懒加载 定义状态
        // lazy private val listState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("list_state", classOf[PageViewCount]))
        lazy val pageViewCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageViewCountMap", classOf[String], classOf[Long]))

        override def processElement(i: PageViewCount, context: KeyedProcessFunction[Long, PageViewCount, String]#Context, collector: Collector[String]): Unit = {
            // listState.add(i)
            pageViewCountMapState.put(i.url, i.count)
            context.timerService().registerEventTimeTimer(i.windowEnd + 1)

            // 额外注册一个定时器，一分钟之后触发，这时候窗口已经彻底关闭，不会再有聚合结果输出
            // 处理 allowedLateness(Time.minutes(1))
            context.timerService().registerEventTimeTimer(i.windowEnd + 60 * 1000)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
            // 定义缓存集合, 注：使用 listState 会出现重复计算的 Bug 所以修正为 MapState
            // val allPageViewCounts: ListBuffer[PageViewCount] = ListBuffer()
            // val iter = listState.get().iterator()
            // while (iter.hasNext) {
            //     allPageViewCounts += iter.next()
            // }

            // 清空状态
            // listState.clear()

            // 判断定时器触发时间，如果已经是窗口结束时间一分钟之后，那么直接情况状态
            // 处理 allowedLateness(Time.minutes(1))
            if (timestamp == ctx.getCurrentKey + 60 * 1000) {
                pageViewCountMapState.clear()
                return
            }

            val iter = pageViewCountMapState.entries().iterator()
            val allPageViewCounts = ListBuffer[(String, Long)]()
            while (iter.hasNext) {
                val entry = iter.next()
                allPageViewCounts += ((entry.getKey, entry.getValue))
            }

            // 按照访问量排序并输出 TopN
            // allPageViewCounts.sortWith(_.count > _.count)
            val sortedPageViewCounts = allPageViewCounts.sortBy(_._2)(Ordering.Long.reverse).take(n)

            // 将排名信息格式化成 String, 便于打印输出可视化展示
            val result = mutable.StringBuilder.newBuilder
            result.append(s"窗口结束时间：${new Timestamp(timestamp - 1)}\n")

            // 遍历 topN 集合封装打印字符串信息
            for (i <- sortedPageViewCounts.indices) {
                val curr = sortedPageViewCounts(i)
                result.append(s"NO${i + 1}: 链接地址=${curr._1}\t 点击量=${curr._2}\n")
            }

            result.append("========================\n\n")

            Thread.sleep(1000L)
            out.collect(result.toString())
        }
    }
}
