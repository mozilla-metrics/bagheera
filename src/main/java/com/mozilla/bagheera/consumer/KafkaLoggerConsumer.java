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
import com.mozilla.bagheera.sink.LoggerSink;
import com.mozilla.bagheera.sink.SinkConfiguration;
import com.mozilla.bagheera.sink.KeyValueSinkFactory;
import com.mozilla.bagheera.util.ShutdownHook;

public class KafkaLoggerConsumer {

private static final Logger LOG = Logger.getLogger(KafkaHBaseConsumer.class);
    
    public static void main(String[] args) {
        OptionFactory optFactory = OptionFactory.getInstance();
        Options options = KafkaConsumer.getOptions();
        options.addOption(optFactory.create("lv", "logvalues", false, "Log values."));
        
        CommandLineParser parser = new GnuParser();
        ShutdownHook sh = ShutdownHook.getInstance();
        try {
            // Parse command line options
            CommandLine cmd = parser.parse(options, args);
            
            final KafkaConsumer consumer = KafkaConsumer.fromOptions(cmd);
            sh.addFirst(consumer);
            
            // Create a sink for storing data
            SinkConfiguration sinkConfig = new SinkConfiguration();
            sinkConfig.setBoolean("loggersink.logvalues", cmd.hasOption("logvalues"));
            KeyValueSinkFactory sinkFactory = KeyValueSinkFactory.getInstance(LoggerSink.class, sinkConfig);
            sh.addLast(sinkFactory);
            
            // Set the sink for consumer storage
            consumer.setSinkFactory(sinkFactory);
            
            // Initialize metrics collection, reporting, etc.
            MetricsManager.getInstance();
            
            // Begin polling
            consumer.poll();
        } catch (ParseException e) {
            LOG.error("Error parsing command line options", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(KafkaHBaseConsumer.class.getName(), options);
        }
    }
    
}
