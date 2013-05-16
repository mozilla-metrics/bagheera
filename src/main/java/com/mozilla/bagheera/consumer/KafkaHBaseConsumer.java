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

import com.mozilla.bagheera.cli.App;
import com.mozilla.bagheera.cli.OptionFactory;
import com.mozilla.bagheera.sink.HBaseSink;
import com.mozilla.bagheera.sink.KeyValueSinkFactory;
import com.mozilla.bagheera.sink.SinkConfiguration;
import com.mozilla.bagheera.util.ShutdownHook;

/**
 * Basic HBase Kafka consumer. This class can be utilized as is but if you want more
 * sophisticated logic consider creating your own consumer.
 */
public final class KafkaHBaseConsumer extends App {

    private static final Logger LOG = Logger.getLogger(KafkaHBaseConsumer.class);

    public static void main(String[] args) {
        OptionFactory optFactory = OptionFactory.getInstance();
        Options options = KafkaConsumer.getOptions();
        options.addOption(optFactory.create("tbl", "table", true, "HBase table name.").required());
        options.addOption(optFactory.create("f", "family", true, "Column family."));
        options.addOption(optFactory.create("q", "qualifier", true, "Column qualifier."));
        options.addOption(optFactory.create("b", "batchsize", true, "Batch size (number of messages per HBase flush)."));
        options.addOption(optFactory.create("pd", "prefixdate", false, "Prefix key with salted date."));

        CommandLineParser parser = new GnuParser();
        ShutdownHook sh = ShutdownHook.getInstance();
        try {
            // Parse command line options
            CommandLine cmd = parser.parse(options, args);

            final KafkaConsumer consumer = KafkaConsumer.fromOptions(cmd);
            sh.addFirst(consumer);

            // Create a sink for storing data
            SinkConfiguration sinkConfig = new SinkConfiguration();
            if (cmd.hasOption("numthreads")) {
                sinkConfig.setInt("hbasesink.hbase.numthreads", Integer.parseInt(cmd.getOptionValue("numthreads")));
            }
            if (cmd.hasOption("batch")) {
                sinkConfig.setInt("hbasesink.hbase.batchsize", Integer.parseInt(cmd.getOptionValue("batch")));
            }
            sinkConfig.setString("hbasesink.hbase.tablename", cmd.getOptionValue("table"));
            sinkConfig.setString("hbasesink.hbase.column.family", cmd.getOptionValue("family", "data"));
            sinkConfig.setString("hbasesink.hbase.column.qualifier", cmd.getOptionValue("qualifier", "json"));
            sinkConfig.setBoolean("hbasesink.hbase.rowkey.prefixdate", cmd.hasOption("prefixdate"));
            KeyValueSinkFactory sinkFactory = KeyValueSinkFactory.getInstance(HBaseSink.class, sinkConfig);
            sh.addLast(sinkFactory);

            // Set the sink factory for consumer storage
            consumer.setSinkFactory(sinkFactory);

            initializeApp();

            // Begin polling
            consumer.poll();
        } catch (ParseException e) {
            LOG.error("Error parsing command line options", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(KafkaHBaseConsumer.class.getName(), options);
        }
    }
}
