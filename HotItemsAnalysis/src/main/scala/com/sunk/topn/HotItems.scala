package com.sunk.topn

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable


/*
 * 使用 FILE, KAFKA, SOCKET source 做实时 topN 统计
 */
object HotItems {

    def main(args: Array[String]): Unit = {
        // 配置执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 定义时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // FILE =========================================================
        // 从文件种读取数据并转换成输入样例类
        // val inputStream: DataStream[String] = env.readTextFile("D:\\workspace\\Java\\Sunk\\FlinkUserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

        // SOCKET =========================================================
        // val inputStream: DataStream[String] = env.socketTextStream("localhost", 9999)

        // KAFKA =========================================================
        // 从 Kafka 中读取数据
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "localhost:9092")
        properties.setProperty("group.id", "group-flink-topn")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        // 创建 source
        val kafka = new FlinkKafkaConsumer[String]("hot_items", new SimpleStringSchema(), properties)
        val inputStream: DataStream[String] = env.addSource(kafka)


        // 提取时间戳，生成 WaterMark（此数据源是有序的所以不指定 WaterMark）
        val dataStream: DataStream[UserBehavior] = inputStream.map(line => {
            val arr = line.split(",")
            UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
        }).assignAscendingTimestamps(_.timestamp * 1000)
        // dataStream.print("DATA")

        // 得到窗口聚合结果
        val aggStream = dataStream
                .filter(_.behavior == "pv")
                .keyBy(_.itemId)
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new CountAgg(), new ItemViewWindowResult())
        // aggStream.print("AGG")

        // 按照每个窗口分组，收集当前窗口内的商品 count 数据再进行自定义的处理
        val resultStream = aggStream.keyBy(_.windowEnd).process(new TopNHotItems(5))
        resultStream.print("RES")

        env.execute()
    }


    // 定义输入数据样例类
    case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)


    // 定义中间输出结果样例类
    case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

    // 自定义预聚合函数 [IN, ACC, OUT]
    class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
        // 初始化累加器
        override def createAccumulator(): Long = 0L

        // 每来一条数据触发一次，这里的逻辑是每一条数据累加器 +1
        override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

        // 获取累加器的方法
        override def getResult(accumulator: Long): Long = accumulator

        // session window 种状态合并
        override def merge(a: Long, b: Long): Long = a + b
    }

    // 自定义窗口聚合函数
    class ItemViewWindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
        override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
            val itemId = key
            val windowEnd = window.getEnd
            val count = input.iterator.next()

            out.collect(ItemViewCount(itemId, windowEnd, count))
        }
    }

    // 对同一个窗口内的所有数据做排序处理
    class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
        // 定义状态 listState
        var itemViewCountListState: ListState[ItemViewCount] = _

        // 在 open 中使用上下文初始化 listState
        override def open(parameters: Configuration): Unit = {
            val itemViewCountListDesc = new ListStateDescriptor[ItemViewCount]("itemViewCountList", classOf[ItemViewCount])
            itemViewCountListState = getRuntimeContext.getListState(itemViewCountListDesc)
        }

        // 每一个元素调用一次
        override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {

            // 将来的数据加入到 listState 中
            itemViewCountListState.add(value)

            // 注册定时器，当窗口关闭 + 1ms 之后触发排序逻辑
            // 每一条数据相同的 windowEnd 注册同一个的定时器，设置 value.windowEnd + 1 是为了等所有的数据都齐了之后延迟 1ms 触发
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
        }

        // 定时器触发时候的操作
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
            // 为了方便排序，将 state 中的数据拿出来
            val allItemViewCounts = scala.collection.mutable.ListBuffer[ItemViewCount]()

            val iter = itemViewCountListState.get().iterator()
            while (iter.hasNext) {
                allItemViewCounts += iter.next()
            }

            // 导出数据之后清空状态
            itemViewCountListState.clear()

            // 按照 count 大小排序, 取前 n 个
            val sortedItemViewCounts = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

            // 将排名信息格式化成 String, 便于打印输出可视化展示
            val result = mutable.StringBuilder.newBuilder
            result.append(s"窗口结束时间：${new Timestamp(timestamp - 1)}\n")

            // 遍历 topN 集合封装打印字符串信息
            for (i <- sortedItemViewCounts.indices) {
                val curr = sortedItemViewCounts(i)
                result.append(s"NO${i + 1}: 商品ID=${curr.itemId}\t 点击量=${curr.count}\t 窗口结束时间=${curr.windowEnd}\n")
            }

            result.append("========================\n\n")

            Thread.sleep(1000L)
            out.collect(result.toString())
        }
    }


}
