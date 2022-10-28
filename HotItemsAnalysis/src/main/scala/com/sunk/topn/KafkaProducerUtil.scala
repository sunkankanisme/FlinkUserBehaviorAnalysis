package com.sunk.topn

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


/*
 * 生成测试数据工具类
 */
object KafkaProducerUtil {

    def main(args: Array[String]): Unit = {
        val filePath = "D:\\workspace\\Java\\Sunk\\FlinkUserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv"

        writeToKafka(filePath, "hot_items")
    }

    def writeToKafka(filePath: String, topicName: String): Unit = {
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "localhost:9092")
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](properties)

        // 从文件读取数据，并逐行写入到 Kafka
        val bufferedSource = io.Source.fromFile(filePath)

        for (line <- bufferedSource.getLines()) {
            val record = new ProducerRecord[String, String](topicName, line)
            producer.send(record)
        }

        producer.close()
    }

}
