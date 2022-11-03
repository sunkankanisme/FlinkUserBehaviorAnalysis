package com.sunk.orderpay

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/*
 * 实时对账，多流 Join
 */
object TxMatchConnect {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 加载 Order 数据流
        val resource = getClass.getResource("/OrderLog.csv")
        val orderInputStream = env.readTextFile(resource.getPath)
                .map(data => {
                    val arr = data.split(",")
                    OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
                })
                .assignAscendingTimestamps(_.timestamp * 1000L)
                .filter(_.eventType == "pay")
                .keyBy(_.txId)

        // 加载到账事件数据
        val receiptResource = getClass.getResource("/ReceiptLog.csv")
        val receiptInputStream = env.readTextFile(receiptResource.getPath)
                .map(data => {
                    val arr = data.split(",")
                    ReceiptEvent(arr(0), arr(1), arr(2).toLong)
                })
                .assignAscendingTimestamps(_.timestamp * 1000L)
                .keyBy(_.txId)


        /*
         * 将两条流合并处理
         *
         * connect: 两种不同数据的流
         * union：一样的流，直接合并
         */
        val tag: OutputTag[(OrderEvent, ReceiptEvent)] = new OutputTag[(OrderEvent, ReceiptEvent)]("TAG")
        val resultStream = orderInputStream.connect(receiptInputStream).process(new TxPayMatchResult(tag))

        resultStream.print("MATCH")
        resultStream.getSideOutput(tag).print("UN_MATCH")

        env.execute()
    }

    // 下单数据样例类
    case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)

    // 到账事件样例类
    case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)

    // 合流之后的处理
    class TxPayMatchResult(tag: OutputTag[(OrderEvent, ReceiptEvent)]) extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
        // 定义状态, 保存当前订单的支付事件和到账事件
        lazy private val payEventState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay", classOf[OrderEvent]))

        lazy private val receiptEventState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))

        /*
         * 处理 OrderEvent 事件
         */
        override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

            // 订单支付事件，判断之前是否有到账事件
            val receipt = receiptEventState.value()

            if (receipt != null) {
                // 如果已经有到账事件，则正常输出匹配，清空状态
                out.collect(value, receipt)
                receiptEventState.clear()
                payEventState.clear()
            } else {
                // 如果还没有到账，则注册定时器开始等待 5s
                ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + 5000L)

                // 更新状态
                payEventState.update(value)
            }
        }

        /*
         * 处理 ReceiptEvent 事件
         */
        override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
            // 到账事件来了，判断之前是否有 pay 事件
            val pay = payEventState.value()

            if (pay != null) {
                // 如果已经有 pay 则正常输出
                out.collect(pay, value)
                receiptEventState.clear()
                payEventState.clear()
            } else {
                // 如果 pay 数据还没有来，则注册定时器等待 3s
                ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + 3000L)

                // 更新状态
                receiptEventState.update(value)
            }
        }

        /*
         * 定时器触发操作
         * - 因为之前没有清空定时器，所以当状态都是空的时候就表示已经正常输出了，啥都不做
         * - 有一个没被清空，则表示已经到的再等另一个，并且超时了，输出到侧输出流
         */
        override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
            // 判断当前状态
            if (payEventState.value() != null || receiptEventState.value() != null) {
                ctx.output(tag, (payEventState.value(), receiptEventState.value()))
            }

            // 清空状态
            payEventState.clear()
            receiptEventState.clear()
        }

    }

}
