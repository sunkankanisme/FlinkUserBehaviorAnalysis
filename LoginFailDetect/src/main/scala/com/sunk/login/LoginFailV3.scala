package com.sunk.login

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/*
 * 使用 CEP 检测连续登录失败的问题
 */
object LoginFailV3 {

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


        // CEP
        // 1 定义一个匹配的模式，要求一个登录失败事件紧跟另一个登录失败事件,并且间隔在 2s 之内
        val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
                .begin[LoginEvent]("first_login_fail").where(_.eventType == "fail")
                // next() 定义紧跟着的一个事件
                .next("second_login_fail").where(_.eventType == "fail")
                // 此处可以跟多个模式
                .within(Time.seconds(2))

        // 2 将模式应用到数据流上，得到新的 PatternStream
        val patternStream: PatternStream[LoginEvent] = CEP.pattern(middleStream.keyBy(_.userId), loginFailPattern)

        // 3 检出符合模式的数据流，需要调用 select 方法
        val loginFailWarningStream = patternStream.select(new LoginFailEventMatch())
        loginFailWarningStream.print()

        env.execute()

    }

    // 定义登录事件输入输出样例类
    case class LoginEvent(userId: Long, ip: String, eventType: String, timestamp: Long)

    // 输出报警信息样例类
    case class LoginFailWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, failTimes: Long, warnMsg: String)

    class LoginFailEventMatch() extends PatternSelectFunction[LoginEvent, LoginFailWarning] {

        /*
         * map 存储方式：
         *
         * first_login_fail -> List[LoginEvent]
         * second_login_fail -> List[LoginEvent]
         *
         */
        override def select(pattern: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {
            // 当前匹配到的事件序列，就保存在 Map 里
            val firstFailEvent = pattern.get("first_login_fail").get(0)
            val secondFailEvent = pattern.get("second_login_fail").get(0)

            LoginFailWarning(firstFailEvent.userId, firstFailEvent.timestamp, secondFailEvent.timestamp, 2, s"login fail ${firstFailEvent}")
        }
    }


}
