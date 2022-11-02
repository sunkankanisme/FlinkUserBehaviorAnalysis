package com.sunk.orderpay

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/*
 * 订单支付超时检测
 */
object OrderTimeoutProcess {

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

        // 1 使用自定义 Process Function 完成复杂事件检测
        val tag = new OutputTag[OrderResult]("TIMEOUT")
        val orderEventStream = keyedInputStream.process(new OrderPayMatchResult(tag))


        // 2 打印输出
        orderEventStream.print("PAY")
        orderEventStream.getSideOutput(tag).print("TIMEOUT")

        env.execute()
    }

    // 定义输入样例类
    case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)

    // 定义输出样例类
    case class OrderResult(orderId: Long, resultMsg: String)


    class OrderPayMatchResult(tag: OutputTag[OrderResult]) extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
        // 定义 bool 状态, 表示 create 和 pay 是否已经来过
        lazy private val isCreatedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isCreated", classOf[Boolean]))
        lazy private val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayed", classOf[Boolean]))

        // 定义 long 状态，表示定时器
        lazy private val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTs", classOf[Long]))


        // 针对每一个元素的处理
        override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
            // 先拿到当前状态
            val isCreated = isCreatedState.value()
            val isPayed = isPayedState.value()
            val timerTs = timerTsState.value()

            /*
             * 判断当前事件的类型，处理乱序的问题（比如 pay 比 create 还先到达）
             *
             * 1 来的是 create，要继续判断是否 pay 过
             *      - 如果已经支付过了，正常支付，输出匹配成功的结果，清空状态和定时器
             *      - 如果未支付，则注册定时器，等待 15 min，并且更新状态
             *
             * 2 来的是 pay，需要判断 create 是否来过
             *      - 如果已经 create 过了，正常顺序的状态，还要判断一下 pay 的时间是否超过了定时器的时间
             *      - 如果 create 每来，产生了乱序数据，注册定时器，等到 pay 的时间就可以
             *
             * 3 定时器触发，有两种超时场景 (create, notPay), (notCrate, payed)
             *
             */
            if (value.eventType == "create") {
                if (isPayed) {
                    out.collect(OrderResult(value.orderId, s"[${value.orderId}] payed successfully"))
                    isCreatedState.clear()
                    isPayedState.clear()
                    timerTsState.clear()
                    ctx.timerService().deleteEventTimeTimer(timerTs)
                } else {
                    val ts = value.timestamp * 1000L + 900 * 1000L
                    ctx.timerService().registerEventTimeTimer(ts)
                    isCreatedState.update(true)
                    timerTsState.update(ts)
                }
            } else if (value.eventType == "pay") {
                if (isCreated) {
                    if (value.timestamp * 1000L < timerTs) {
                        out.collect(OrderResult(value.orderId, s"[${value.orderId}] payed successfully"))
                    } else {
                        // 已超时的数据输出到侧输出流
                        ctx.output(tag, OrderResult(value.orderId, s"[${value.orderId}] payed but already timeout"))
                    }

                    // 只要输出结果，当前 order 已经结束清空状态和定时器
                    isCreatedState.clear()
                    isPayedState.clear()
                    timerTsState.clear()
                    ctx.timerService().deleteEventTimeTimer(timerTs)
                } else {
                    // 等到 watermark 触发
                    ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L)

                    // 更新状态
                    timerTsState.update(value.timestamp * 1000L)
                    isPayedState.update(true)
                }
            }
        }

        // 定时器触发逻辑
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
            if (isPayedState.value()) {
                // (notCrate, payed)
                ctx.output(tag, OrderResult(ctx.getCurrentKey, s"[${ctx.getCurrentKey}] payed but not found create"))
            } else {
                // (create, notPay)
                ctx.output(tag, OrderResult(ctx.getCurrentKey, s"[${ctx.getCurrentKey}] created, order timeout"))
            }

            // 清空状态
            isPayedState.clear()
            isCreatedState.clear()
            timerTsState.clear()
        }

    }

}
