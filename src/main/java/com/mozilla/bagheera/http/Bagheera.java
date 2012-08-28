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
package com.mozilla.bagheera.http;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.mozilla.bagheera.metrics.MetricsManager;
import com.mozilla.bagheera.producer.KafkaProducer;
import com.mozilla.bagheera.producer.Producer;
import com.mozilla.bagheera.util.WildcardProperties;

public class Bagheera {

    private static final Logger LOG = Logger.getLogger(Bagheera.class);
    
    public static final String PROPERTIES_RESOURCE_NAME = "/bagheera.properties";
    private static final int DEFAULT_IO_THREADS = Runtime.getRuntime().availableProcessors() * 2;

    static final ChannelGroup allChannels = new DefaultChannelGroup(Bagheera.class.getName());
    
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getProperty("server.port", "8080"));
        boolean tcpNoDelay = Boolean.parseBoolean(System.getProperty("server.tcpnodelay", "false"));
        
        // Initialize metrics collection, reporting, etc.
        MetricsManager.getInstance();
        
        // Initalize properties and producer
        WildcardProperties props = new WildcardProperties();
        Properties kafkaProps = new Properties();
        InputStream in = null;
        try {
            URL propUrl = Bagheera.class.getResource(PROPERTIES_RESOURCE_NAME);
            if (propUrl == null) {
                throw new IllegalArgumentException("Could not find the properites file: " + PROPERTIES_RESOURCE_NAME);
            }
            in = propUrl.openStream();
            props.load(in);
            in.close();
            
            propUrl = Bagheera.class.getResource("/kafka.producer.properties");
            if (propUrl == null) {
                throw new IllegalArgumentException("Could not find the properites file: " + "/kafka.properties");
            }
            
            in = propUrl.openStream();
            kafkaProps.load(in);
        } finally {
            if (in != null) {
                in.close();
            }
        }
        final Producer producer = new KafkaProducer(kafkaProps);
        
        // HTTP server setup
        final NioServerSocketChannelFactory channelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(), Executors.newFixedThreadPool(DEFAULT_IO_THREADS)) {
            @Override
            public void releaseExternalResources() {
                super.releaseExternalResources();
                if (producer != null) {
                    LOG.info("Closing producer resource...");
                    try {
                        producer.close();
                    } catch (IOException e) {
                        LOG.error("Error closing producer", e);
                    }
                }
            }
        };
        ServerBootstrap sb = new ServerBootstrap(channelFactory);
        HttpServerPipelineFactory pipeFactory;
        try {
            pipeFactory = new HttpServerPipelineFactory(props, producer);
            sb.setPipelineFactory(pipeFactory);
            sb.setOption("tcpNoDelay", tcpNoDelay);
            sb.setOption("keepAlive", false);
            Channel ch = sb.bind(new InetSocketAddress(port));
            allChannels.add(ch);
            Runtime.getRuntime().addShutdownHook(new Thread() {
               public void run() {
                   ChannelGroupFuture future = allChannels.close();
                   future.awaitUninterruptibly();
                   channelFactory.releaseExternalResources();
               }
            });
        } catch (IOException e) {
            LOG.error("Error initializing pipeline factory", e);
        }
    }

}
