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
package com.mozilla.bagheera.rest.nio;

import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import redis.clients.jedis.JedisPool;

public class BagheeraNIO extends NioServerSocketChannelFactory {

	private JedisPool jedisPool;
	
	public BagheeraNIO(ExecutorService bossExecutor, Executor workerExecutor) {
		this(bossExecutor, workerExecutor, 10);
	}
	
	public BagheeraNIO(ExecutorService bossExecutor, Executor workerExecutor, int workerCount) {
		super(bossExecutor, workerExecutor, workerCount);
		Config jedisConfig = new Config();
		jedisConfig.maxActive = 50;
		this.jedisPool = new JedisPool(jedisConfig, "localhost");
	}
	
	public JedisPool getJedisPool() {
		return jedisPool;
	}
	
	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory#releaseExternalResources()
	 */
	public void releaseExternalResources() {
		if (jedisPool != null) {
			jedisPool.destroy();
		}
	}
	
	public static void main(String[] args) {
		// Configure the server.
		BagheeraNIO bnio = new BagheeraNIO(Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), 100);
		ServerBootstrap bootstrap = new ServerBootstrap(bnio);

		// Set up the event pipeline factory.
		bootstrap.setPipelineFactory(new HttpServerPipelineFactory(bnio.getJedisPool()));
		
		// Bind and start to accept incoming connections.
		int port = Integer.parseInt(System.getProperty("server.port", "8080"));
		bootstrap.bind(new InetSocketAddress(port));
	}
}
