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
package com.mozilla.bagheera.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.mozilla.bagheera.metrics.HazelcastMonitor;
import com.mozilla.bagheera.metrics.MetricsManager;

public class BagheeraNio {

    private static final Logger LOG = Logger.getLogger(BagheeraNio.class);
    
    public static final String PROPERTIES_RESOURCE_NAME = "/bagheera.properties";
    private static final int DEFAULT_IO_THREADS = Runtime.getRuntime().availableProcessors() * 2;

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getProperty("server.port", "8080"));
        boolean tcpNoDelay = Boolean.parseBoolean(System.getProperty("server.tcpnodelay", "false"));
        
        // Initialize metrics collection, reporting, etc.
        MetricsManager.getInstance();
        
        // Initialize Hazelcast now rather than waiting for the first request
        //Config config = new Config();
        //config.setInstanceName("bagheera");
        HazelcastInstance hzInstance = Hazelcast.newHazelcastInstance(null);
        for (Map.Entry<String, MapConfig> entry : hzInstance.getConfig().getMapConfigs().entrySet()) {
            String mapName = entry.getKey();
            // If the map contains a wildcard then we need to wait to initialize
            if (!mapName.contains("*")) {
                hzInstance.getMap(entry.getKey());
            }
        }
        
        // Setup the Hazelcast Monitor
        HazelcastMonitor hzMonitor = HazelcastMonitor.getInstance(hzInstance);
        hzMonitor.monitor();
        
        // HTTP
        NioServerSocketChannelFactory channelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(), Executors.newFixedThreadPool(DEFAULT_IO_THREADS));
        ServerBootstrap sb = new ServerBootstrap(channelFactory);
        HttpServerPipelineFactory pipeFactory;
        try {
            pipeFactory = new HttpServerPipelineFactory(hzInstance.getConfig().getMapConfigs().keySet(), hzInstance);
            sb.setPipelineFactory(pipeFactory);
            sb.setOption("tcpNoDelay", tcpNoDelay);
            sb.setOption("keepAlive", false);
            sb.bind(new InetSocketAddress(port));
        } catch (IOException e) {
            LOG.error("Error initializing pipeline factory", e);
        }
    }

}
