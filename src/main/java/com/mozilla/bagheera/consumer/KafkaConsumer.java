/*
 * Copyright 2012 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.bagheera.consumer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.apache.commons.cli.CommandLine;
import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;
import com.mozilla.bagheera.sink.KeyValueSink;

public class KafkaConsumer implements Consumer {

    private static final Logger LOG = Logger.getLogger(KafkaConsumer.class);
    
    private static final int DEFAULT_NUM_THREADS = 2;

    private ConsumerConnector consumerConnector;
    private List<KafkaStream<Message>> streams;
    private ExecutorService executor;
    private KeyValueSink sink;
    
    public KafkaConsumer(String topic, Properties props) {
        this(topic, props, DEFAULT_NUM_THREADS);
    }
    
    public KafkaConsumer(String topic, Properties props, int numThreads) {
        LOG.info("# of threads: " + numThreads);
        executor = Executors.newFixedThreadPool(numThreads);
        
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
        streams = consumerConnector.createMessageStreamsByFilter(new Whitelist(topic), numThreads);
    }
    
    public void setSink(KeyValueSink sink) {
        this.sink = sink;
    }
    
    public void close() {
        LOG.info("Shutting down!");
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    LOG.info("Shutting down now!");
                    if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                        LOG.error("Unable to shudown consumer thread pool");
                    }
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        // close the kafka consumer connector
        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }
    }
    
    public void poll() throws InterruptedException,ExecutionException {
        List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
        for (final KafkaStream<Message> stream : streams) {  
            tasks.add(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    try {
                        BagheeraMessage bmsg = null;
                        for (MessageAndMetadata<Message> mam : stream) {
                            bmsg = BagheeraMessage.parseFrom(ByteString.copyFrom(mam.message().payload()));                         
                            sink.store(bmsg.getId(), bmsg.getPayload().toByteArray());
                        }
                    } catch (InvalidProtocolBufferException e) {
                        LOG.error("Invalid protocol buffer in data stream", e);
                    } catch (UnsupportedEncodingException e) {
                        LOG.error("Message ID was not in UTF-8 encoding", e);
                    } catch (IOException e) {
                        LOG.error("IO error while storing to data sink", e);
                    }
                    
                    return true;
                }
            });
        }

        List<Future<Boolean>> futures = executor.invokeAll(tasks);
        for (Future<Boolean> future : futures) {
            future.get();
        }
    }
    
    public static KafkaConsumer fromOptions(CommandLine cmd) {
        Properties props = new Properties();
        String propsFilePath = cmd.getOptionValue("properties");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(propsFilePath)));
            props.load(reader);
            props.setProperty("groupid", cmd.getOptionValue("groupid"));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        
        int numThreads = props.containsKey("consumer.threads") ? Integer.parseInt(props.getProperty("consumer.threads")) : DEFAULT_NUM_THREADS;
        
        return new KafkaConsumer(cmd.getOptionValue("topic"), props, numThreads);
    }
    
}
