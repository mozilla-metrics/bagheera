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

import java.util.concurrent.ExecutionException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.mozilla.bagheera.cli.OptionFactory;
import com.mozilla.bagheera.metrics.MetricsManager;
import com.mozilla.bagheera.sink.HBaseSink;
import com.mozilla.bagheera.sink.KeyValueSink;
import com.mozilla.bagheera.util.ShutdownHook;

/**
 * Basic HBase Kafka consumer. This class can be utilized as is but if you want more
 * sophisticated logic consider creating your own consumer.
 */
public final class KafkaHBaseConsumer {

    private static final Logger LOG = Logger.getLogger(KafkaHBaseConsumer.class);
    
    public static void main(String[] args) {
        OptionFactory optFactory = OptionFactory.getInstance();
        Options options = KafkaConsumer.getOptions();
        options.addOption(optFactory.create("tbl", "table", true, "HBase table name.").required());
        options.addOption(optFactory.create("f", "family", true, "Column family."));
        options.addOption(optFactory.create("q", "qualifier", true, "Column qualifier."));
        options.addOption(optFactory.create("pd", "prefixdate", false, "Prefix key with salted date."));
        
        CommandLineParser parser = new GnuParser();
        ShutdownHook sh = ShutdownHook.getInstance();
        try {
            // Parse command line options
            CommandLine cmd = parser.parse(options, args);
            
            final KafkaConsumer consumer = KafkaConsumer.fromOptions(cmd);
            sh.addFirst(consumer);
            
            // Create a sink for storing data
            final KeyValueSink sink = new HBaseSink(cmd.getOptionValue("table"), 
                                                    cmd.getOptionValue("family", "data"), 
                                                    cmd.getOptionValue("qualifier", "json"), 
                                                    Boolean.parseBoolean(cmd.getOptionValue("prefixdate", "true")));
            sh.addLast(sink);
            
            // Set the sink for consumer storage
            consumer.setSink(sink);
            
            // Initialize metrics collection, reporting, etc.
            MetricsManager.getInstance();
            
            // Begin polling
            consumer.poll();
        } catch (ParseException e) {
            LOG.error("Error parsing command line options", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(KafkaHBaseConsumer.class.getName(), options);
        } catch (InterruptedException e) {
            LOG.error("Interrupted while polling", e);
        } catch (ExecutionException e) {
            LOG.error("ExecutionException while polling", e);
        }
    }
}
