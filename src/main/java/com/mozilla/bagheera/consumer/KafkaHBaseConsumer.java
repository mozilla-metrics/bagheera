package com.mozilla.bagheera.consumer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;
import com.mozilla.bagheera.cli.OptionFactory;
import com.mozilla.bagheera.sink.KeyValueSink;
import com.mozilla.bagheera.sink.LoggerSink;

public class KafkaHBaseConsumer implements Consumer {

    private static final Logger LOG = Logger.getLogger(KafkaHBaseConsumer.class);
    
    private static final int DEFAULT_NUM_THREADS = 1;

    private List<KafkaStream<Message>> streams;
    private ExecutorService executor;
    private KeyValueSink sink;
    
    public KafkaHBaseConsumer(String topic, Properties props, KeyValueSink sink, int numThreads) {
        this.sink = sink;
        
        LOG.info("# of threads: " + numThreads);
        executor = Executors.newFixedThreadPool(numThreads);
        
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
        consumerConnector.createMessageStreamsByFilter(new Whitelist(topic), numThreads);
        Map<String, List<KafkaStream<Message>>> topicMessageStreams = consumerConnector.createMessageStreams(Collections.singletonMap(topic, numThreads));
        streams = topicMessageStreams.get(topic);
    }
    
    public void close() {
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                        LOG.error("Unable to shudown consumer thread pool");
                    }
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        // TODO: Figure out if we need to close something in Kafka...probably do
        sink.close();
    }
    
    public void poll() {
        for (final KafkaStream<Message> stream : streams) {
            try {
                BagheeraMessage bmsg = null;
                for (MessageAndMetadata<Message> mam : stream) {
                    Message msg = mam.message();
                    bmsg = BagheeraMessage.parseFrom(ByteString.copyFrom(msg.payload()));                         
                    sink.store(bmsg.getId().getBytes("UTF-8"), bmsg.getPayload().toByteArray());
                }
            } catch (Exception e) {
                System.err.println("Exception: " + e.getMessage());
            }
            /*
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        BagheeraMessage bmsg = null;
                        for (MessageAndMetadata<Message> mam : stream) {
                            Message msg = mam.message();
                            bmsg = BagheeraMessage.parseFrom(ByteString.copyFrom(msg.payload()));                         
                            sink.store(bmsg.getId().getBytes("UTF-8"), bmsg.getPayload().toByteArray());
                        }
                    } catch (Exception e) {
                        System.err.println("Exception: " + e.getMessage());
                    }
                }
            });
            */
        }
    }
    
    public static void main(String[] args) {
        OptionFactory optFactory = OptionFactory.getInstance();
        Options options = new Options();
        options.addOption(optFactory.create("n", "namespace", true, "Namespace to poll.").required());
        options.addOption(optFactory.create("gid", "groupid", true, "Kafka group ID.").required());
        options.addOption(optFactory.create("p", "properties", true, "Kafka consumer properties file.").required());

        options.addOption(new Option("t", "table", true, "HBase table name."));
        options.addOption(new Option("f", "family", true, "Column family."));
        options.addOption(new Option("q", "qualifier", true, "Column qualifier."));
        options.addOption(new Option("pd", "prefixdate", false, "Prefix key with salted date."));
        
        CommandLineParser parser = new GnuParser();
        KafkaHBaseConsumer consumer = null;
        BufferedReader reader = null;
        try {
            // Parse command line options
            CommandLine cmd = parser.parse(options, args);
            
            // Create a sink for storing data
            /*
            KeyValueSink sink = new HBaseSink(cmd.getOptionValue("table"), 
                                              cmd.getOptionValue("family"), 
                                              cmd.getOptionValue("qualifier"), true);
            */
            KeyValueSink sink = new LoggerSink(true);
            
            // Load consumer properties file
            String propsFilePath = cmd.getOptionValue("properties");
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(propsFilePath)));
            Properties props = new Properties();
            props.load(reader);
            props.setProperty("groupid", cmd.getOptionValue("groupid"));
            
            // Create the consumer and start polling
            consumer = new KafkaHBaseConsumer(cmd.getOptionValue("namespace"), 
                                              props, sink, DEFAULT_NUM_THREADS);
            consumer.poll();
        } catch (ParseException e) {
            LOG.error("Error parsing command line options", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("HBaseConsumer", options);
        } catch (IOException e) {
            LOG.error("Error loading consumer properties", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    LOG.error("Error closing properties file reader", e);
                }
            }
            if (consumer != null) {
                consumer.close();
            }
        }
    }
}
