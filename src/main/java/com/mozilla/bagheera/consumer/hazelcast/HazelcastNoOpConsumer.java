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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.mozilla.bagheera.consumer.Consumer;

public class HazelcastNoOpConsumer implements Consumer {

    private static final Logger LOG = Logger.getLogger(HazelcastNoOpConsumer.class);

    private long sleepTime = 1000L;
    
    // Hazelcast related member vars
    private IMap<String, String> nsMap;
    
    public HazelcastNoOpConsumer(String namespace, String hzGroupName, String hzGroupPassword, String[] hzClients) {
        ClientConfig config = new ClientConfig();
        config.addAddress(hzClients);
        config.getGroupConfig().setName(hzGroupName).setPassword(hzGroupPassword);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        nsMap = client.getMap(namespace);
    }
    
    public void poll() {
        try {
            while (true) {
                if (!nsMap.isEmpty()) {
                    for (String k : nsMap.keySet()) {
                        String v = nsMap.remove(k);
                        if (v != null) {
                            LOG.info(k + " => " + v.length());
                        }
                    }
                } else {
                    Thread.sleep(sleepTime);
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted while polling", e);
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ParseException {
        Options options = new Options();
        Option mapName = new Option("m", "map", true, "Name of map in Hazelcast.");
        mapName.setRequired(true);
        options.addOption(mapName);
        options.addOption(new Option("gn", "groupname", true, "Hazelcast group name."));
        options.addOption(new Option("gp", "grouppassword", true, "Hazelcast group password."));
        options.addOption(new Option("hzservers", true, "Hazelcast server list."));
        
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = parser.parse(options, args);
        HazelcastNoOpConsumer consumer = new HazelcastNoOpConsumer(cmd.getOptionValue("map"), 
                                                 cmd.getOptionValue("groupname","bagheera"), 
                                                 cmd.getOptionValue("grouppassword","bagheera"), 
                                                 cmd.getOptionValue("hzservers","localhost:5701").split(","));
        consumer.poll();
    }
}
