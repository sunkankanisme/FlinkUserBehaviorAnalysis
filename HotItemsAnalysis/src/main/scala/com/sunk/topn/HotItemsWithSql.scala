package com.sunk.topn

import com.sunk.topn.HotItems.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/*
 * 使用 TableApi 实现 topN
 */
object HotItemsWithSql {

    def main(args: Array[String]): Unit = {

        // 初始化执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 定义表执行环境
        val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
        val tableEnv = StreamTableEnvironment.create(env, settings)

        // 使用 FileSource
        val inputStream: DataStream[String] = env.readTextFile("HotItemsAnalysis/src/main/resources/UserBehavior.csv")

        // 提取时间戳，生成 WaterMark（此数据源是有序的所以不指定 WaterMark）
        val dataStream: DataStream[UserBehavior] = inputStream.map(line => {
            val arr = line.split(",")
            UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
        }).assignAscendingTimestamps(_.timestamp * 1000)


        // 基于 DataStream 创建 Table
        // val dataTable = tableEnv.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)
        // 1 Table API 进行开窗聚合统计
        // val aggTable = dataTable.filter('behavior === "pv")
        //         .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
        //         .groupBy('itemId, 'sw)
        //         .select('itemId, 'sw.end as 'windowEnd, 'itemId.count as 'cnt)


        // 1 使用 SQL 完成第一步
        tableEnv.createTemporaryView("dataTable", dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)
        val aggTable = tableEnv.sqlQuery("select itemId, hop_end(ts, interval '5' minute, interval '1' hour) windowEnd, count(itemId) cnt from dataTable where behavior = 'pv' group by itemId,hop(ts, interval '5' minute, interval '1' hour)")

        // 2 用 sql 实现 topN 的选取
        tableEnv.createTemporaryView("aggTable", aggTable, 'itemId, 'windowEnd, 'cnt)
        val resultTable = tableEnv.sqlQuery(s"select * from (select *,row_number() over(partition by windowEnd order by cnt desc) as row_num from aggTable) tmp where row_num <= 5")

        resultTable.toRetractStream[Row].print()

        env.execute("HotItemsWithSql")
    }

}
