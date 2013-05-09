/*
 * Copyright 2013 Mozilla Foundation
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
import com.mozilla.bagheera.sink.KeyValueSinkFactory;
import com.mozilla.bagheera.sink.ReplaySink;
import com.mozilla.bagheera.sink.SinkConfiguration;
import com.mozilla.bagheera.util.ShutdownHook;

/**
 * Kafka consumer which reads from one kafka queue and re-creates requests to send elsewhere.
 */
public final class KafkaReplayConsumer extends App {

    private static final Logger LOG = Logger.getLogger(KafkaReplayConsumer.class);

    public static void main(String[] args) {
        OptionFactory optFactory = OptionFactory.getInstance();
        Options options = KafkaConsumer.getOptions();
        options.addOption(optFactory.create("k", "copy-keys", true, "Whether or not to copy keys from the source data").required());
        options.addOption(optFactory.create("d", "dest", true, "Destination host / url pattern (include '" + ReplaySink.KEY_PLACEHOLDER + "' for key placeholder)"));
        options.addOption(optFactory.create("s", "sample", true, "Rate at which to sample the source data (defaults to using all data)"));
        options.addOption(optFactory.create("D", "delete", true, "Also replay deletes (using the source keys by necessity)"));


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
            sinkConfig.setString("replaysink.keys", cmd.getOptionValue("copy-keys", "true"));
            sinkConfig.setString("replaysink.dest", cmd.getOptionValue("dest", "http://bogus:8080/submit/endpoint/" + ReplaySink.KEY_PLACEHOLDER));
            sinkConfig.setString("replaysink.sample", cmd.getOptionValue("sample", "1"));
            sinkConfig.setString("replaysink.delete", cmd.getOptionValue("delete", "true"));
            KeyValueSinkFactory sinkFactory = KeyValueSinkFactory.getInstance(ReplaySink.class, sinkConfig);
            sh.addLast(sinkFactory);

            // Set the sink factory for consumer storage
            consumer.setSinkFactory(sinkFactory);

            prepareHealthChecks();

            // Begin polling
            consumer.poll();
        } catch (ParseException e) {
            LOG.error("Error parsing command line options", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(KafkaReplayConsumer.class.getName(), options);
        }
    }
}
