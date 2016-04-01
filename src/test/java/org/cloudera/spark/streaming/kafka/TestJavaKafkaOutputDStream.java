/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.cloudera.spark.streaming.kafka;

import com.google.common.collect.Lists;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.cloudera.spark.streaming.kafka.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class TestJavaKafkaOutputDStream {
  private TestUtil testUtil = TestUtil.getInstance();
  // Name of the framework for Spark context
  private String framework = this.getClass().getSimpleName();

  // Master for Spark context
  private String master = "local[2]";
  private SparkConf conf = new SparkConf()
    .setMaster(master)
    .setAppName(framework);
  //  conf.set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
  private JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(2000l));

  @Before
  public void setup() {
    testUtil.prepare();
    List<String> topics = new ArrayList<String>(3);
    topics.add("default");
    topics.add("static");
    topics.add("custom");
    testUtil.initTopicList(topics);
  }

  @After
  public void tearDown() {
    testUtil.tearDown();
  }

  @Test
  public void testKafkaDStream() throws Exception {
    Queue<JavaRDD<String>> toBe = new LinkedList<JavaRDD<String>>();
    int j = 0;
    while (j < 9) {
      toBe.add(ssc.sc().parallelize(Lists.newArrayList(
        Integer.toString(j), Integer.toString(j + 1), Integer.toString(j + 2))));
      j += 3;
    }
    JavaDStream<String> instream = ssc.queueStream(toBe);
    Properties producerConf = new Properties();
    producerConf.put("serializer.class", "kafka.serializer.DefaultEncoder");
    producerConf.put("key.serializer.class", "kafka.serializer.StringEncoder");
    producerConf.put("metadata.broker.list", testUtil.getKafkaServerUrl());
    producerConf.put("request.required.acks", "1");
    JavaDStreamKafkaWriter.writeToKafka(instream, producerConf, new ProcessingFunc());
    ssc.start();
    Thread.sleep(10000);
    int i = 0;
    Set<String> expectedResults =
      new TreeSet<String>(Lists.newArrayList("0", "1", "2", "3", "4", "5", "6", "7", "8"));
    Set<String> actualResults = new TreeSet<String>();
    boolean moreMessages = true;
    while (moreMessages) {
      MessageAndMetadata msg = testUtil.getNextMessageFromConsumer("default");
      if (msg != null) {
        String fetchedMsg = new String((byte[]) msg.message());
        Assert.assertNotNull(fetchedMsg);
        actualResults.add(fetchedMsg);
      } else {
        moreMessages = false;
      }
    }
    Assert.assertEquals(expectedResults, actualResults);
  }
}

  class ProcessingFunc implements Function<String, KeyedMessage<String, byte[]>> {

    public KeyedMessage<String, byte[]> call(String in) throws Exception {
      return new KeyedMessage<String, byte[]>("default", null, in.getBytes());
    }
  }
