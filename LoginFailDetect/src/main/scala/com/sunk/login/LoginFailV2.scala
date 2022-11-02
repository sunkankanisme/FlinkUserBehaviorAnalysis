package com.sunk.login

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/*
 * 改进版
 */
object LoginFailV2 {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 读取数据
        val resource = getClass.getResource("/LoginLogTest.csv")
        val inputStream = env.readTextFile(resource.getPath)

        // 转换成样例类
        val loginEventStream = inputStream.map(data => {
            val arr = data.split(",")
            LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
        })

        // 提取时间戳, 设置 WaterMark
        val middleStream = loginEventStream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
                    override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000
                })

        val result = middleStream.keyBy(_.userId).process(new LoginFailResult())
        result.print()

        env.execute()
    }

    // 定义登录事件输入输出样例类
    case class LoginEvent(userId: Long, ip: String, eventType: String, timestamp: Long)

    // 输出报警信息样例类
    case class LoginFailWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, failTimes: Long, warnMsg: String)

    /*
     * 不使用定时器
     */
    class LoginFailResult() extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {
        // 定义状态保存当前所有的登录失败事件
        lazy private val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login_fail_state", classOf[LoginEvent]))

        // 处理每一条数据
        override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
            println(s"PROCESS: $value, ${loginFailState.get()}")

            // 判断登录事件类型
            if (value.eventType == "fail") {
                // 在当前是失败登录的情况下
                val iter = loginFailState.get().iterator()

                /*
                 * 判断之前是否有登录失败事件
                 *
                 * 如果之前已经有失败事件，则判断两次登录时间差
                 * 如果没有则将当前登录失败事件存入状态集合中
                 */
                if (iter.hasNext) {
                    val firstFailEvent = iter.next()
                    // 在 2s 之内连续 2 此失败
                    if (value.timestamp <= firstFailEvent.timestamp + 2L) {
                        out.collect(LoginFailWarning(ctx.getCurrentKey, firstFailEvent.timestamp, value.timestamp, 2, s"${ctx.getCurrentKey} login fail 2 times in 2 seconds."))
                    }

                    // 不管报不报警，当前已处理完毕，将状态更新为最近一次登录失败的事件
                    loginFailState.clear()
                    loginFailState.add(value)
                } else {
                    loginFailState.add(value)
                }
            } else {
                // 成功登录清空状态
                loginFailState.clear()
            }
        }
    }

}
