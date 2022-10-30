package com.sunk.market

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/*
 * App 分渠道统计 (下载或者浏览)
 */
object AppMarketByChannel {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 定义输入数据源
        val source = new SimulatedSource()
        val inputStream: DataStream[MarketUserBehavior] = env.addSource(source)
                .assignAscendingTimestamps(_.timestamp)

        // 开窗统计输出
        val resultStream = inputStream.filter(_.behavior != "uninstall")
                .keyBy(userBehavior => (userBehavior.behavior, userBehavior.channel))
                .timeWindow(Time.days(1), Time.seconds(5))
                .process(new MarketCountByChannel())

        // 打印输出
        resultStream.print()

        env.execute()
    }

    // 定义输入数据样例类
    case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

    // 定义输出数据样例类
    case class MarketViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

    /*
     * 自定义测试数据源
     */
    class SimulatedSource() extends RichSourceFunction[MarketUserBehavior] {
        // 定义是否运行的标志位
        var running = true

        // 定义用户行为和渠道的集合
        val behaviorSet: Seq[String] = Seq("view", "click", "download", "install", "uninstall")
        val channelSet: Seq[String] = Seq("appstore", "weibo", "wechat", "tieba")

        // 定义随机数生成器
        val rand: Random = Random

        /*
         * 随机生成数据并发送出去
         */
        override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
            // 定义生成数据的最大数量
            val maxCount = Long.MaxValue

            // 当前生成的条数
            var count = 0L

            // 生成数据
            while (running && count <= maxCount) {
                val id = UUID.randomUUID().toString
                val behavior = behaviorSet(rand.nextInt(behaviorSet.size - 1))
                val channel = channelSet(rand.nextInt(channelSet.size - 1))
                val ts = System.currentTimeMillis()

                ctx.collect(MarketUserBehavior(id, behavior = behavior, channel = channel, timestamp = ts))
                count += 1

                // 休眠一段时间
                Thread.sleep(50)
            }
        }

        /*
         * 在什么情况下取消生成数据
         */
        override def cancel(): Unit = {
            running = false
        }
    }

    /*
     * 自定义 ProcessWindowFunction
     */
    class MarketCountByChannel() extends ProcessWindowFunction[MarketUserBehavior, MarketViewCount, (String, String), TimeWindow] {

        /*
         * 数据齐了之后触发此方法
         */
        override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketViewCount]): Unit = {
            val winStart = new Timestamp(context.window.getStart).toString
            val winEnd = new Timestamp(context.window.getEnd).toString
            val behavior = key._1
            val channel = key._2
            val count = elements.size

            out.collect(MarketViewCount(winStart, winEnd, channel, behavior, count))
        }
    }

}
