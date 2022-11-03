package com.sunk.orderpay

import com.sunk.orderpay.OrderTimeout.OrderEvent
import com.sunk.orderpay.TxMatchConnect.ReceiptEvent
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/*
 * 使用 Join 完成对账
 */
object TxMatchJoin {

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

        // intervalJoin
        val resultStream = orderInputStream.intervalJoin(receiptInputStream)
                .between(Time.seconds(-3), Time.seconds(5))
                .process(new TxMatchJoinFunction())

        resultStream.print()

        env.execute()
    }

    class TxMatchJoinFunction() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

        override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
            out.collect(left, right)
        }

    }


}
