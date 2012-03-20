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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;

import com.mozilla.bagheera.metrics.MetricsManager;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;

public class BagheeraNio {

    private static final Logger LOG = Logger.getLogger(BagheeraNio.class);
    
    public static final String PROPERTIES_RESOURCE_NAME = "/bagheera.properties";
    private static final int DEFAULT_IO_THREADS = Runtime.getRuntime().availableProcessors() * 2;

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getProperty("server.port", "8080"));

        /* setup metrics collection, reporting etc */
        MetricsManager.getInstance().run();
        // Initialize Hazelcast now rather than waiting for the first request
        Hazelcast.getDefaultInstance();
        Config config = Hazelcast.getConfig();
        for (Map.Entry<String, MapConfig> entry : config.getMapConfigs().entrySet()) {
            String mapName = entry.getKey();
            // If the map contains a wildcard then we need to wait to initialize
            if (!mapName.contains("*")) {
                Hazelcast.getMap(entry.getKey());
            }
        }
        
        // HTTP
        NioServerSocketChannelFactory channelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(), Executors.newFixedThreadPool(DEFAULT_IO_THREADS));
        ServerBootstrap sb = new ServerBootstrap(channelFactory);
        HttpServerPipelineFactory pipeFactory;
        try {
            pipeFactory = new HttpServerPipelineFactory(config.getMapConfigs().keySet());
            sb.setPipelineFactory(pipeFactory);
            sb.setOption("tcpNoDelay", true);
            sb.setOption("keepAlive", false);
            sb.bind(new InetSocketAddress(port));
        } catch (IOException e) {
            LOG.error("Error initializing pipeline factory", e);
        }
    }

}
