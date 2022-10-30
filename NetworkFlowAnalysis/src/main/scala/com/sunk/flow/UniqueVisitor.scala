package com.sunk.flow

import com.sunk.flow.PageView.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object UniqueVisitor {

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
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountResult())

        UvStream.print()
        env.execute()
    }

    // 实现自定义全窗口函数
    class UvCountResult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {

        /*
         * input 是窗口内的所有数据
         */
        override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
            println("=== APPLY: " + input.size + " " + input.head)

            // 统计 Uv 时考虑使用 set 集合存储所有的 UserId 进行去重
            var userIdSet = Set[Long]()

            // 遍历窗口中的所有数据，将 uid 添加到 set
            for (userBehavior <- input) {
                userIdSet += userBehavior.userId
            }

            out.collect(UvCount(window.getEnd, userIdSet.size))
        }
    }

    // 定义中间输出样例类
    case class UvCount(windowEnd: Long, count: Long)

}
