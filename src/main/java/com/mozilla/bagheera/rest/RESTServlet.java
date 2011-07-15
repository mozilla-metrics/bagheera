/*
 * Copyright 2010 The Apache Software Foundation
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
package com.mozilla.bagheera.rest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.rest.Constants;
import org.apache.hadoop.hbase.rest.metrics.RESTMetrics;

/**
 * Singleton class encapsulating global REST servlet state and functions.
 */
public class RESTServlet implements Constants {

	private static final int DEFAULT_POOL_SIZE = 10;
	private static RESTServlet INSTANCE;
	private final Configuration conf;
	private final HTablePool tablePool;
	private final RESTMetrics metrics = new RESTMetrics();

	/**
	 * @return the RESTServlet singleton instance
	 * @throws IOException
	 */
	public synchronized static RESTServlet getInstance() throws IOException {
		assert (INSTANCE != null);
		return INSTANCE;
	}

	/**
	 * @param conf Existing configuration to use in rest servlet
	 * @return the RESTServlet singleton instance
	 * @throws IOException
	 */
	public synchronized static RESTServlet getInstance(Configuration conf) throws IOException {
		if (INSTANCE == null) {
			INSTANCE = new RESTServlet(conf, DEFAULT_POOL_SIZE);
		}
		
		return INSTANCE;
	}
	
	/**
	 * @param conf Existing configuration to use in rest servlet
	 * @param poolSize The size of the HTablePool
	 * @return
	 * @throws IOException
	 */
	public synchronized static RESTServlet getInstance(Configuration conf, int poolSize) throws IOException {
		if (INSTANCE == null) {
			INSTANCE = new RESTServlet(conf, poolSize);
		}
		
		return INSTANCE;
	}

	public synchronized static void stop() {
		if (INSTANCE != null) {
			INSTANCE = null;
		}
	}

	/**
	 * Constructor with existing configuration
	 * 
	 * @param conf
	 * @param numThreads
	 * @throws IOException
	 */
	private RESTServlet(Configuration conf, int poolSize) throws IOException {
		this.conf = conf;
		this.tablePool = new HTablePool(conf, poolSize);
	}

	public HTablePool getTablePool() {
		return tablePool;
	}

	public Configuration getConfiguration() {
		return conf;
	}
	
	public RESTMetrics getMetrics() {
		return metrics;
	}

	/**
	 * Helper method to determine if server should only respond to GET HTTP
	 * method requests.
	 * 
	 * @return boolean for server read-only state
	 */
	public boolean isReadOnly() {
		return getConfiguration().getBoolean("hbase.rest.readonly", false);
	}
}
