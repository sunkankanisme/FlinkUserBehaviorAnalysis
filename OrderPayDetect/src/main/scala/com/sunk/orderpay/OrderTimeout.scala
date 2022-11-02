package com.sunk.orderpay

import java.sql.Timestamp
import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/*
 * 订单支付超时检测
 */
object OrderTimeout {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 0. 加载数据源
        val resource = getClass.getResource("/OrderLog.csv")
        val keyedInputStream: KeyedStream[OrderEvent, Long] = env.readTextFile(resource.getPath)
                .map(data => {
                    val arr = data.split(",")
                    OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
                })
                .assignAscendingTimestamps(_.timestamp * 1000L)
                .keyBy(_.orderId)

        // 1. 定义 CEP 模式
        val pattern = Pattern.begin[OrderEvent]("create").where(_.eventType == "create")
                .followedBy("pay").where(_.eventType == "pay")
                .within(Time.minutes(15))

        // 2. 将 pattern 应用到数据流上进行模式匹配
        val patternStream: PatternStream[OrderEvent] = CEP.pattern(keyedInputStream, pattern)

        // 3. 定义侧输出流标签，用于将超时未匹配的数据输出到侧输出流
        val orderTimeoutOutputTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("orderTimeout")

        // 4. 调用 select 方法，提取并处理匹配的成功支付事件，以及超时事件
        val resultStream = patternStream.select(orderTimeoutOutputTag,
            new OrderTimeoutSelect(),
            new OrderPaySelect())

        // 5. 打印输出
        resultStream.print("PAY")
        resultStream.getSideOutput(orderTimeoutOutputTag).print("TIMEOUT")

        env.execute()
    }

    // 定义输入样例类
    case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)

    // 定义输出样例类
    case class OrderResult(orderId: Long, resultMsg: String)

    // 超时未匹配处理函数
    class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
        override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
            val createEvent = pattern.get("create").get(0)
            OrderResult(createEvent.orderId, s"order ${createEvent.orderId} start at ${new Timestamp(createEvent.timestamp * 1000L)} and timeout at ${new Timestamp(timeoutTimestamp)}")
        }
    }

    // 正常匹配到的处理函数
    class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
        override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
            val createEvent = pattern.get("create").get(0)
            val payEvent = pattern.get("pay").get(0)

            OrderResult(createEvent.orderId, s"order ${createEvent.orderId} start at ${new Timestamp(createEvent.timestamp * 1000L)} and payed at ${new Timestamp(payEvent.timestamp * 1000L)}")
        }
    }


}
