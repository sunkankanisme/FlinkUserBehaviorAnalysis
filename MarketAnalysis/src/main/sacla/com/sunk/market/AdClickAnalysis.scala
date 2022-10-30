package com.sunk.market

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
 * 广告点击量相关统计
 * 增加黑名单逻辑
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

        // 插入进一步过滤的操作，并将有刷单行为的用户输出到侧输出流（黑名单报警）
        // 针对同一个用户点击同一个广告来进行处理，所以使用 (log.userId, log.adId) 分组
        val filterBlackListUserStream = adLogStream.keyBy(log => (log.userId, log.adId))
                .process(new FilterBlacklistUserResult(100))

        // 开窗聚合统计
        val resultStream = filterBlackListUserStream.keyBy(_.province)
                .timeWindow(Time.days(1), Time.seconds(5))
                .aggregate(new AdCountAgg(), new AdCountWindowResult())

        // 打印统计结果信息
        resultStream.print("RESULT")
        // 打印侧输出流中的 Warn 信息
        filterBlackListUserStream.getSideOutput(new OutputTag[BlacklistUserWarning]("WARN")).print("WARN")

        env.execute()
    }

    // 定义输入输出样例类
    case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

    case class AdClickCountByProvince(windowStart: String, windowEnd: String, province: String, count: Long)

    // 侧输出流中的黑名单报警信息样例类
    case class BlacklistUserWarning(userId: Long, adId: Long, msg: String)

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

    /*
     * 黑名单报警逻辑
     * - 状态编程
     * - 设置定时器
     *
     */
    class FilterBlacklistUserResult(maxCount: Long) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
        // 定义状态，保存用户对广告的点击量
        private lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))

        // 定义每天 0 点定时清空状态的时间戳
        private lazy val resetTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("result_ts", classOf[Long]))

        // 定义一个 bool 状态，保存当前 key（用户-广告） 是否已经在黑名单中了
        private lazy val isBlackState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isBlackState", classOf[Boolean]))


        override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
            // 1 首先获取到当前的状态
            val currCount = countState.value()

            // 2 注册定时器, 判断只要是第一个数据来了，直接注册 0 点的清空状态定时器
            if (currCount == 0) {
                // 计算明天 0 点的用于设置定时器
                // 毫秒时间戳 / 1000 表示秒时间戳，除 (1000 * 60 * 60 * 24) 表示天级时间戳, +1 天之后再乘 () 转换为时间戳
                // 由于时间区域的原因，所以需要再提前 8 个小时来触发, - (8 * 60 * 60 * 1000)
                val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000) - (8 * 60 * 60 * 1000)
                resetTimerTsState.update(ts)

                ctx.timerService().registerProcessingTimeTimer(ts)
            }

            // 3 判断 count 值是否已经达到了定义的阈值，如果超过就输出到侧输出流
            if (currCount >= maxCount) {
                // 判断是否已经在黑名单里了，没有的话才输出
                if (!isBlackState.value()) {
                    isBlackState.update(true)
                    ctx.output(new OutputTag[BlacklistUserWarning]("WARN"), BlacklistUserWarning(value.userId, value.adId, s"Click Over $maxCount times today."))
                }

                return
            }

            // 4 正常情况下 count +1，并且正常输出数据
            countState.update(currCount + 1)
            out.collect(value)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
            if (timestamp == resetTimerTsState.value()) {
                isBlackState.clear()
                countState.clear()
            }
        }
    }
}
