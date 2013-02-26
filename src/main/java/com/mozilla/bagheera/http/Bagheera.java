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

/**
 * Front-end class to a Bagheera server instance.
 *
 * Either create a server using `startServer`, or allow the main method to do so.
 */
public class Bagheera {

    private static final Logger LOG = Logger.getLogger(Bagheera.class);

    public static final String PROPERTIES_RESOURCE_NAME = "/bagheera.properties";
    public static final String KAFKA_PROPERTIES_RESOURCE_NAME = "/kafka.producer.properties";

    private static final int DEFAULT_IO_THREADS = Runtime.getRuntime().availableProcessors() * 2;

    // Ensure that we only do this once.
    private static boolean metricsManagerInitialized = false;

    public static NioServerSocketChannelFactory getChannelFactory() {
        return new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                                                 Executors.newFixedThreadPool(DEFAULT_IO_THREADS));
    }

    /**
     * Immutable record of the persistent state around a Bagheera server.
     */
    public static class BagheeraServerState {
        public final int port;
        public final Producer producer;
        public final NioServerSocketChannelFactory channelFactory;
        public final Channel channel;
        public final ChannelGroup channelGroup;

        public BagheeraServerState(final int port,
                                   final Producer producer,
                                   final NioServerSocketChannelFactory channelFactory,
                                   final Channel channel,
                                   final ChannelGroup channelGroup) {
            this.port = port;
            this.producer = producer;
            this.channelFactory = channelFactory;
            this.channel = channel;
            this.channelGroup = channelGroup;
        }

        public void close() {
            // Close our channels.
            this.channelGroup.close().awaitUninterruptibly();
            this.channel.close().awaitUninterruptibly();

            // The caller is responsible for releasing resources from the channel factory.

            // Shut down producer.
            if (this.producer != null) {
                LOG.info("Closing producer resource...");
                try {
                    this.producer.close();
                } catch (IOException e) {
                    LOG.error("Error closing producer.", e);
                }
            }
        }
    }

    /**
     * Start a Bagheera server with the provided settings.
     * Throws if the server could not be started.
     * The caller is responsible for closing the returned instance, and the
     * channel factory if desired.
     */
    public static BagheeraServerState startServer(final int port,
                                                  final boolean tcpNoDelay,
                                                  final WildcardProperties props,
                                                  final Producer producer,
                                                  final NioServerSocketChannelFactory channelFactory,
                                                  final String channelGroupName)
        throws Exception {

        // Initialize metrics collection, reporting, etc.
        // We assume that this method is thread-safe.
        // Do this only once.
        if (!metricsManagerInitialized) {
          MetricsManager.getInstance();
          metricsManagerInitialized = true;
        }

        // HTTP server setup.
        final ChannelGroup channelGroup = new DefaultChannelGroup(channelGroupName);
        final ServerBootstrap server = new ServerBootstrap(channelFactory);
        final HttpServerPipelineFactory pipeFactory = new HttpServerPipelineFactory(props, producer, channelGroup);
        server.setPipelineFactory(pipeFactory);
        server.setOption("tcpNoDelay", tcpNoDelay);

        // Disable keep-alive so client connections don't hang around.
        server.setOption("keepAlive", false);

        final Channel channel = server.bind(new InetSocketAddress(port));
        return new BagheeraServerState(port, producer, channelFactory, channel, channelGroup);
    }

    /**
     * A simple front-end that configures a new server from properties files,
     * waiting until runtime shutdown to clean up.
     */
    public static void main(String[] args) throws Exception {
        final int port = Integer.parseInt(System.getProperty("server.port", "8080"));
        final boolean tcpNoDelay = Boolean.parseBoolean(System.getProperty("server.tcpnodelay", "false"));

        // Initalize properties and producer.
        final WildcardProperties props = getDefaultProperties();
        final Properties kafkaProps = getDefaultKafkaProperties();
        final Producer producer = new KafkaProducer(kafkaProps);

        final BagheeraServerState server = startServer(port,
                                                       tcpNoDelay,
                                                       props,
                                                       producer,
                                                       getChannelFactory(),
                                                       Bagheera.class.getName());

        Runtime.getRuntime().addShutdownHook(new Thread() {
           public void run() {
               server.close();
               server.channelFactory.releaseExternalResources();
           }
        });
    }

    protected static Properties getDefaultKafkaProperties() throws Exception {
        final Properties props = new Properties();
        final URL propUrl = Bagheera.class.getResource(KAFKA_PROPERTIES_RESOURCE_NAME);
        if (propUrl == null) {
            throw new IllegalArgumentException("Could not find the properties file: " + KAFKA_PROPERTIES_RESOURCE_NAME);
        }

        final InputStream in = propUrl.openStream();
        try {
            props.load(in);
        } finally {
            in.close();
        }

        return props;
    }

    protected static WildcardProperties getDefaultProperties() throws Exception {
        final WildcardProperties props = new WildcardProperties();
        final URL propUrl = Bagheera.class.getResource(PROPERTIES_RESOURCE_NAME);
        if (propUrl == null) {
            throw new IllegalArgumentException("Could not find the properties file: " + PROPERTIES_RESOURCE_NAME);
        }

        final InputStream in = propUrl.openStream();
        try {
            props.load(in);
        } finally {
            in.close();
        }

        return props;
    }
}
