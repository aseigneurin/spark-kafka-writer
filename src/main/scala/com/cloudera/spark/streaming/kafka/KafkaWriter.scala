/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.spark.streaming.kafka

import java.util.Properties

import kafka.producer.{ProducerConfig, KeyedMessage, Producer}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
 * Import this object in this form:
 * {{{
 *   import org.apache.spark.streaming.kafka.KafkaWriter._
 * }}}
 *
 * Once this is done, the `writeToKafka` can be called on the [[DStream]] object in this form:
 * {{{
 *   dstream.writeToKafka(producerConfig, f)
 * }}}
 */
object KafkaWriter {
  /**
   * This implicit method allows the user to call dstream.writeToKafka(..)
   * @param dstream - DStream to write to Kafka
   * @tparam T - The type of the DStream
   * @tparam K - The type of the key to serialize to
   * @tparam V - The type of the value to serialize to
   * @return
   */
  implicit def createKafkaOutputWriter[T: ClassTag, K, V](
    dstream: DStream[T]): KafkaWriter[T] = {
    new KafkaWriter[T](dstream)
  }
}

/**
 *
 * This class can be used to write data to Kafka from Spark Streaming. To write data to Kafka
 * simply `import org.apache.spark.streaming.kafka.KafkaWriter._` in your application and call
 * `dstream.writeToKafka(producerConf, func)`
 *
 * Here is an example:
 * {{{
 * // Adding this line allows the user to call dstream.writeDStreamToKafka(..)
 * import org.apache.spark.streaming.kafka.KafkaWriter._
 *
 * class ExampleWriter {
 *   val instream = ssc.queueStream(toBe)
 *   val producerConf = new Properties()
 *   producerConf.put("serializer.class", "kafka.serializer.DefaultEncoder")
 *   producerConf.put("key.serializer.class", "kafka.serializer.StringEncoder")
 *   producerConf.put("metadata.broker.list", "kafka.example.com:5545")
 *   producerConf.put("request.required.acks", "1")
 *   instream.writeToKafka(producerConf,
 *   (x: String) => new KeyedMessage[String,String]("default", null, x))
 *   ssc.start()
 * }
 *
 * }}}
 * @param dstream - The [[DStream]] to be written to Kafka
 *
 */
class KafkaWriter[T: ClassTag](@transient dstream: DStream[T]) {

  /**
   * To write data from a DStream to Kafka, call this function after creating the DStream. Once
   * the DStream is passed into this function, all data coming from the DStream is written out to
   * Kafka. The properties instance takes the configuration required to connect to the Kafka
   * brokers in the standard Kafka format. The serializerFunc is a function that converts each
   * element of the RDD to a Kafka [[KeyedMessage]]. This closure should be serializable - so it
   * should use only instances of Serializables.
   * @param producerConfig The configuration that can be used to connect to Kafka
   * @param serializerFunc The function to convert the data from the stream into Kafka
   *                       [[KeyedMessage]]s.
   * @tparam K The type of the key
   * @tparam V The type of the value
   *
   */
  def writeToKafka[K, V](producerConfig: Properties,
    serializerFunc: T => KeyedMessage[K, V]): Unit = {
    // Broadcast the producer to avoid sending it every time.
    val broadcastedConfig = dstream.context.sparkContext.broadcast(producerConfig)
    def func = (rdd: RDD[T]) => {
      rdd.foreachPartition(events => {
        // The ForEachDStream runs the function locally on the driver. So the
        // ProducerObject from the driver is likely to get serialized and
        // sent, which is fine - because at that point the Producer itself is
        // not initialized, so a None is sent over the wire.
        // Get the producer from that local executor and write!
        val producer: Producer[K, V] = {
          if (ProducerObject.isCached) {
            ProducerObject.getCachedProducer
              .asInstanceOf[Producer[K, V]]
          } else {
            val producer =
              new Producer[K, V](new ProducerConfig(broadcastedConfig.value))
            ProducerObject.cacheProducer(producer)
            producer
          }
        }
        producer.send(events.map(serializerFunc).toArray: _*)
      })
    }
    dstream.foreachRDD(func)
  }

}
