/*
 * Copyright 2011 Mozilla Foundation
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
package com.mozilla.bagheera.consumer.hazelcast;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.mozilla.bagheera.consumer.SequenceFileConsumer;

public class HazelcastSequenceFileConsumer extends SequenceFileConsumer {

    private static final Logger LOG = Logger.getLogger(HazelcastSequenceFileConsumer.class);
    
    // Hazelcast map
    private Map<String, String> nsMap;

    public HazelcastSequenceFileConsumer(String namespace, String baseDirPath, String dateFormat, long maxFileSize,
                                String hzGroupName, String hzGroupPassword, String[] hzClients) throws IOException {
        super(namespace, baseDirPath, dateFormat, maxFileSize);    
        
        ClientConfig config = new ClientConfig();
        config.addAddress(hzClients);
        config.getGroupConfig().setName(hzGroupName).setPassword(hzGroupPassword);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        nsMap = client.getMap(namespace);
    }
    
    public void poll() {
        try {
            while (true) {
                if (nsMap.size() > 0) {
                    long startTime = System.currentTimeMillis(); 
                    try {
                        checkRollover();
                        for (String k : nsMap.keySet()) {
                            String v = nsMap.remove(k);
                            if (v != null) {
                                outputKey.set(k);
                                bytesWritten += outputKey.getLength();
                                outputValue.set(v);
                                bytesWritten += outputValue.getLength();
                                writer.append(outputKey, outputValue);
                            }
                        }
                    } catch (IOException e) {
                        LOG.error("IOException while writing key/value pair", e);
                        throw new RuntimeException(e);
                    }
                    
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Stored %d items in %d ms", nsMap.size(), (System.currentTimeMillis() - startTime)));
                    }
                } else {
                    Thread.sleep(sleepTime);
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted while polling", e);
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException {
        Options options = new Options();
        Option mapName = new Option("m", "map", true, "Name of map in Hazelcast.");
        mapName.setRequired(true);
        options.addOption(mapName);
        options.addOption(new Option("o", "outputdir", true, "Base output directory path."));
        options.addOption(new Option("df", "dateformat", true, "Output subdirectories date format."));
        options.addOption(new Option("fs", "filesize", true, "Maximum output file size."));
        options.addOption(new Option("gn", "groupname", true, "Hazelcast group name."));
        options.addOption(new Option("gp", "grouppassword", true, "Hazelcast group password."));
        options.addOption(new Option("hzservers", true, "Hazelcast server list."));
        
        CommandLineParser parser = new GnuParser();
        HazelcastSequenceFileConsumer consumer = null;
        try {
            CommandLine cmd = parser.parse(options, args);
            consumer = new HazelcastSequenceFileConsumer(mapName.getValue(), 
                                                cmd.getOptionValue("outputdir", "/bagheera"),
                                                cmd.getOptionValue("dateformat", "yyyy-MM-dd"),
                                                Long.parseLong(cmd.getOptionValue("filesize", "1073741824")),
                                                cmd.getOptionValue("groupname","bagheera"), cmd.getOptionValue("grouppassword","bagheera"),
                                                cmd.getOptionValue("hzservers","localhost:5701").split(","));
            consumer.poll();            
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("SequenceFileConsumer", options);
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }
    
}
