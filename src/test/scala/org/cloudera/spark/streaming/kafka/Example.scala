package org.cloudera.spark.streaming.kafka

import java.util.Properties

import kafka.producer.KeyedMessage
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Example {

  val checkpointDir = "/tmp/serialization-issue"

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("serialization-issue")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val ssc = StreamingContext.getOrCreate(checkpointDir, createStreamingContext(sc) _)

    ssc.start()
    ssc.awaitTermination()
  }

  def createStreamingContext(sc: SparkContext)() = {
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint(checkpointDir)

    val producerConfig = new Properties
    producerConfig.put("metadata.broker.list", "localhost:9092")
    producerConfig.put("serializer.class", "kafka.serializer.StringEncoder")
    producerConfig.put("key.serializer.class", "kafka.serializer.StringEncoder")
    producerConfig.put("request.required.acks", "1")

    val dstream = ssc.socketTextStream("localhost", 9123)
    DStreamKafkaWriter.writeToKafka(dstream, producerConfig, serializerFunc)

    ssc
  }

  private def serializerFunc(s: String): KeyedMessage[String, String] = {
    KeyedMessage("output", null, null, s)
  }

}
