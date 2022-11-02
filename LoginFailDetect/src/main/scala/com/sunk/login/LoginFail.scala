package com.sunk.login

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object LoginFail {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 读取数据
        val resource = getClass.getResource("/LoginLog.csv")
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

        // 进行判断和检测，如果 2s 之内连续登录失败则输出报警信息
        val warningStream = middleStream.keyBy(_.userId).process(new LoginFailWarnResult(2))
        warningStream.print()

        env.execute()
    }


    // 定义登录事件输入输出样例类
    case class LoginEvent(userId: Long, ip: String, eventType: String, timestamp: Long)

    // 输出报警信息样例类
    case class LoginFailWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, failTimes: Long, warnMsg: String)

    class LoginFailWarnResult(failTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {
        // 定义状态保存当前所有的登录失败事件
        lazy private val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login_fail_state", classOf[LoginEvent]))

        // 定义状态保存定时器的时间戳
        lazy private val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer_ts", classOf[Long]))

        override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
            println(s"PROCESS ELE: $value")

            // 判断当前登录事件是成功还是失败
            if (value.eventType == "fail") {
                loginFailState.add(value)

                // 当前 key 没有定时器的情况下注册一个新的定时器
                if (timerTsState.value() == 0) {
                    // 如果没有定时器，则注册一个 2s 之后的定时器
                    val ts = value.timestamp * 1000L + 2000
                    ctx.timerService().registerEventTimeTimer(ts)
                    timerTsState.update(ts)
                }
            } else {
                // 如果是成功，那么直接清空状态和定时器，重新开始
                ctx.timerService().deleteEventTimeTimer(timerTsState.value())
                loginFailState.clear()
                timerTsState.clear()
            }
        }

        // 定时器触发操作
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {
            println(s"ONTIMER: $timestamp")

            // 定义一个集合缓存当前状态里的数据
            val allLoginFailList: ListBuffer[LoginEvent] = ListBuffer()

            val iter = loginFailState.get().iterator()
            while (iter.hasNext) {
                allLoginFailList += iter.next()
            }

            // 判断登录失败事件的个数，如果超过了上线则报警
            val realLoginFailTimes = allLoginFailList.size
            if (realLoginFailTimes >= failTimes) {
                out.collect(LoginFailWarning(ctx.getCurrentKey, allLoginFailList.head.timestamp, allLoginFailList.last.timestamp, realLoginFailTimes, s"login fail in 2s for $realLoginFailTimes times."))
            }


            // 清空状态
            loginFailState.clear()
            timerTsState.clear()
        }
    }

}
