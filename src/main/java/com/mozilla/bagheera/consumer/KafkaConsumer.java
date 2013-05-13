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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;
import com.mozilla.bagheera.BagheeraProto.BagheeraMessage.Operation;
import com.mozilla.bagheera.cli.OptionFactory;
import com.mozilla.bagheera.consumer.validation.JsonValidator;
import com.mozilla.bagheera.consumer.validation.ValidationPipeline;
import com.mozilla.bagheera.sink.KeyValueSink;
import com.mozilla.bagheera.sink.KeyValueSinkFactory;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public class KafkaConsumer implements Consumer {

    private static final Logger LOG = Logger.getLogger(KafkaConsumer.class);
    
    protected static final int DEFAULT_NUM_THREADS = 2;

    protected ExecutorService executor;
    protected List<Future<Void>> workers;
    protected ConsumerConnector consumerConnector;
    protected List<KafkaStream<Message>> streams;
    protected KeyValueSinkFactory sinkFactory;
    protected ValidationPipeline validationPipeline;
    
    protected Meter consumed;
    protected Meter invalidMessageMeter;
    
    public KafkaConsumer(String topic, Properties props) {
        this(topic, props, DEFAULT_NUM_THREADS);
    }
    
    public KafkaConsumer(String topic, Properties props, int numThreads) {
        LOG.info("# of threads: " + numThreads);
        executor = Executors.newFixedThreadPool(numThreads);
        workers = new ArrayList<Future<Void>>(numThreads);
        
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
        streams = consumerConnector.createMessageStreamsByFilter(new Whitelist(topic), numThreads);
        
        consumed = Metrics.newMeter(new MetricName("bagheera", "consumer", topic + ".consumed"), "messages", TimeUnit.SECONDS);
        invalidMessageMeter = Metrics.newMeter(new MetricName("bagheera", "consumer", topic + ".invalid"), "messages", TimeUnit.SECONDS);
    }

    public void setSinkFactory(KeyValueSinkFactory sinkFactory) {
        this.sinkFactory = sinkFactory;
    }
    
    public void setValidationPipeline(ValidationPipeline pipeline) {
        this.validationPipeline = pipeline;
    }
    
    @Override
    public void close() {
        LOG.info("Shutting down!");
        if (executor != null) {
            // Regular shutdown doesn't do much for us here since
            // these are long running threads
            executor.shutdown();
            try {
                // To actually interrupt our workers we'll cancel each future.
                for (Future<?> worker : workers) {
                    worker.cancel(true);
                }
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    LOG.info("Shutting down now!");
                    if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                        LOG.error("Unable to shudown consumer thread pool");
                    }
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            } finally {
                // close the kafka consumer connector
                if (consumerConnector != null) {
                    LOG.info("Shutting down consumer connector!");
                    consumerConnector.shutdown();
                }
            }
        } 
    }
    
    @Override
    public void poll() {
        final CountDownLatch latch = new CountDownLatch(streams.size());
        for (final KafkaStream<Message> stream : streams) {  
            workers.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() {                  
                    try {
                        for (MessageAndMetadata<Message> mam : stream) {
                            BagheeraMessage bmsg = BagheeraMessage.parseFrom(ByteString.copyFrom(mam.message().payload()));
                            // get the sink for this message's namespace 
                            // (typically only one sink unless a regex pattern was used to listen to multiple topics)
                            KeyValueSink sink = sinkFactory.getSink(bmsg.getNamespace());
                            if (sink == null) {
                                LOG.error("Could not obtain sink for namespace: " + bmsg.getNamespace());
                                break;
                            }
                            if (bmsg.getOperation() == Operation.CREATE_UPDATE && 
                                bmsg.hasId() && bmsg.hasPayload()) {
                                if (validationPipeline == null ||
                                    validationPipeline.isValid(bmsg.getPayload().toByteArray())) {
                                    if (bmsg.hasTimestamp()) {
                                        sink.store(bmsg.getId(), bmsg.getPayload().toByteArray(), bmsg.getTimestamp());
                                    } else {
                                        sink.store(bmsg.getId(), bmsg.getPayload().toByteArray());
                                    }
                                } else {
                                    invalidMessageMeter.mark();
                                    // TODO: sample out an example payload
                                    LOG.warn("Invalid payload for namespace: " + bmsg.getNamespace());
                                }
                            } else if (bmsg.getOperation() == Operation.DELETE &&
                                bmsg.hasId()) {
                                sink.delete(bmsg.getId());
                            }
                            consumed.mark();            
                        }
                    } catch (InvalidProtocolBufferException e) {
                        LOG.error("Invalid protocol buffer in data stream", e);
                    } catch (UnsupportedEncodingException e) {
                        LOG.error("Message ID was not in UTF-8 encoding", e);
                    } catch (IOException e) {
                        LOG.error("IO error while storing to data sink", e);
                    } finally {
                    	latch.countDown();
                    }
                    
                    return null;
                }
            }));
        }

        // Wait for all tasks to complete which in the normal case they will
        // run indefinitely unless we detect that a thread exited
        try {
            while (true) {
                latch.await(10, TimeUnit.SECONDS);
                if (latch.getCount() != streams.size()) {
                    // we have a dead thread and should exit
                    break;
                }
            }
        } catch (InterruptedException e) {
            LOG.info("Interrupted during polling", e);
        }
        
        // Spit out errors if there were any
        for (Future<Void> worker : workers) {
            try {
                if (worker.isDone() && !worker.isCancelled()) {
                    worker.get(1, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                LOG.error("Thread was interrupted:", e);
            } catch (ExecutionException e) {
                LOG.error("Exception occured in thread:", e);
            } catch (TimeoutException e) {
                LOG.error("Timed out waiting for thread result:", e);
            } catch (CancellationException e) {
                LOG.error("Thread has been canceled: ", e);
            }
       }
    }
    
    /**
     * Get the set of common command-line options for a Kafka consumer
     * @return
     */
    public static Options getOptions() {
        OptionFactory optFactory = OptionFactory.getInstance();
        Options options = new Options();
        options.addOption(optFactory.create("t", "topic", true, "Topic to poll.").required());
        options.addOption(optFactory.create("gid", "groupid", true, "Kafka group ID.").required());
        options.addOption(optFactory.create("p", "properties", true, "Kafka consumer properties file.").required());
        options.addOption(optFactory.create("nt", "numthreads", true, "Number of consumer threads."));
        options.addOption(optFactory.create("vj", "validatejson", false, "Validate payload as JSON."));
        return options;
    }
    
    /**
     * Create a KafkaConsumer from the given command-line options
     * @param cmd
     * @return
     */
    public static KafkaConsumer fromOptions(CommandLine cmd) {
        Properties props = new Properties();
        String propsFilePath = cmd.getOptionValue("properties");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(propsFilePath)));
            props.load(reader);
            props.setProperty("groupid", cmd.getOptionValue("groupid"));
        } catch (FileNotFoundException e) {
            LOG.error("Could not find properties file", e);
        } catch (IOException e) {
            LOG.error("Error reading properties file", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    LOG.error("Error closing properties file", e);
                }
            }
        }
        
        int numThreads = props.containsKey("consumer.threads") ? Integer.parseInt(props.getProperty("consumer.threads")) : DEFAULT_NUM_THREADS;
        // if numthreads specified on command-line then override
        if (cmd.hasOption("numthreads")) {
            numThreads = Integer.parseInt(cmd.getOptionValue("numthreads"));
        }
        
        // construct consumer
        KafkaConsumer consumer = new KafkaConsumer(cmd.getOptionValue("topic"), props, numThreads);
        
        // setup validation pipeline if we need to
        if (cmd.hasOption("validatejson")) {
            ValidationPipeline vp = new ValidationPipeline();
            vp.addFirst(new JsonValidator());
            consumer.setValidationPipeline(vp);
        }
        
        return consumer;
    }
    
}
