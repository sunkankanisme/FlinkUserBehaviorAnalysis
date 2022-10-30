package com.sunk.flow

import com.sunk.flow.PageView.UserBehavior
import com.sunk.flow.UniqueVisitor.UvCount
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UvWithBloom {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
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

        // timeWindowAll 尽量不要使用 timeWindowAll
        val UvStream = dataStream
                .filter(_.behavior == "pv")
                .map(data => ("pv", data.userId))
                .keyBy(_._1)
                .timeWindow(Time.hours(1))
                .trigger(new MyTrigger())
                .process(new UvCountWithBloom())

        UvStream.print()
        env.execute()
    }

    /*
     * 自定义触发器，每来一条数据直接触发窗口计算，并清空窗口状态
     */
    class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
        /*
         * 当来一条新数据的时候
         */
        override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
            TriggerResult.FIRE_AND_PURGE
        }

        /*
         * 系统时间有进展的时候
         */
        override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

        /*
         * 收到 WaterMark
         */
        override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

        override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}
    }

    /*
     * 自己实现一个简单博隆过滤器
     * - 主要是一个位图和 hash 函数
     */
    class Bloom(size: Long) extends Serializable {
        private val cap = size

        // hash 函数
        def hash(value: String, seed: Int): Long = {
            var res = 0L

            for (i <- 0 until value.length) {
                res = res * seed + value.charAt(i)
            }

            // 返回 Hash，要映射到 cap 范围内
            (cap - 1) & res
        }
    }

    // 实现自定义的窗口处理函数
    class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
        // 定义 Redis 连接以及布隆过滤器
        lazy val jedis = new Jedis("localhost", 6379)
        // 位的个数：2^6 * 2^20(1M) * 2^3(8bit) = 64M
        lazy val bloomFilter = new Bloom(1 << 29)


        // 原本所有数据收集齐了，会触发计算
        // 此处由于自定义 trigger 所以每来一条都调用一次
        override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
            // 定义 Redis 存储 bit map 的 key
            val storedBitMapKey = context.window.getEnd.toString

            // 另外将当前窗口的 uv count 值作为状态保存到 redis 里，用一个 uvcount 的 hash 表来保存（windowEnd，count）
            val uvCountMap = "uvcount"
            val currentKey = context.window.getEnd.toString
            var count = 0L

            // 从 redis 中取出当前窗口的 uv count 值
            val value = jedis.hget(uvCountMap, currentKey)
            if (value != null) {
                count = value.toLong
            }

            // 去重判断当前 userId 的 hash 值对应的位图位置，是否为 0
            val userId = elements.head._2.toString
            // 计算 hash 值，对应着位图中的偏移量
            val offset = bloomFilter.hash(userId, 61)
            // 用 redis 的位操作命令，取 bitmap 中的对应值
            val isExists = jedis.getbit(storedBitMapKey, offset)
            // 如果不存在，则表示当前 userId 是新的
            if (!isExists) {
                jedis.setbit(storedBitMapKey, offset, true)
                jedis.hset(uvCountMap, currentKey, (count + 1).toString)
            }
        }
    }
}
