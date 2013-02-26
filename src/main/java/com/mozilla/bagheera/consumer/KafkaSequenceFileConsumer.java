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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.mozilla.bagheera.cli.OptionFactory;
import com.mozilla.bagheera.metrics.MetricsManager;
import com.mozilla.bagheera.sink.SequenceFileSink;
import com.mozilla.bagheera.sink.SinkConfiguration;
import com.mozilla.bagheera.sink.KeyValueSinkFactory;
import com.mozilla.bagheera.util.ShutdownHook;

/**
 * Basic SequenceFile (HDFS) Kafka consumer. This class can be utilized as is but if you want more
 * sophisticated logic consider creating your own consumer.
 */
public final class KafkaSequenceFileConsumer {

    private static final Logger LOG = Logger.getLogger(KafkaSequenceFileConsumer.class);
    
    public static void main(String[] args) {
        OptionFactory optFactory = OptionFactory.getInstance();
        Options options = KafkaConsumer.getOptions();
        options.addOption(optFactory.create("o", "output", true, "HDFS base path for output."));
        options.addOption(optFactory.create("df", "dateformat", true, "Date format for the date subdirectories."));
        options.addOption(optFactory.create("fs", "filesize", true, "Max file size for output files."));
        options.addOption(optFactory.create("b", "usebytes", false, "Use BytesWritable for value rather than Text."));
        
        CommandLineParser parser = new GnuParser();
        ShutdownHook sh = ShutdownHook.getInstance();
        try {
            // Parse command line options
            CommandLine cmd = parser.parse(options, args);
            
            final KafkaConsumer consumer = KafkaConsumer.fromOptions(cmd);
            sh.addFirst(consumer);
            
            // Create a sink for storing data
            SinkConfiguration sinkConfig = new SinkConfiguration();
            sinkConfig.setString("hdfssink.hdfs.basedir.path", cmd.getOptionValue("output", "/bagheera"));
            sinkConfig.setString("hdfssink.hdfs.date.format", cmd.getOptionValue("dateformat", "yyyy-MM-dd"));
            sinkConfig.setLong("hdfssink.hdfs.max.filesize", Long.parseLong(cmd.getOptionValue("filesize", "536870912")));
            sinkConfig.setBoolean("hdfssink.hdfs.usebytes", cmd.hasOption("usebytes"));
            KeyValueSinkFactory sinkFactory = KeyValueSinkFactory.getInstance(SequenceFileSink.class, sinkConfig);
            sh.addLast(sinkFactory);
            
            // Set the sink for consumer storage
            consumer.setSinkFactory(sinkFactory);
            
            // Initialize metrics collection, reporting, etc.
            MetricsManager.configureMetricsManager();
            
            // Begin polling
            consumer.poll();
        } catch (ParseException e) {
            LOG.error("Error parsing command line options", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(KafkaSequenceFileConsumer.class.getName(), options);
        } catch (NumberFormatException e) {
            LOG.error("Failed to parse filesize option", e);
        }
    }
}

