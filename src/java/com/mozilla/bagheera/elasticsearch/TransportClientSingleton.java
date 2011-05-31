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
package com.mozilla.bagheera.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class TransportClientSingleton {

	public static int DEFAULT_TRANSPORT_SOCKET = 9300;
	
	private static TransportClientSingleton INSTANCE;
	
	private final Client client;
	
	private TransportClientSingleton(String[] servers) {
		TransportClient tc = new org.elasticsearch.client.transport.TransportClient();
		for (String server : servers) {
			tc.addTransportAddress(new InetSocketTransportAddress(server, DEFAULT_TRANSPORT_SOCKET));
		}
		client = tc;
	}
	
	public static TransportClientSingleton getInstance(String[] servers) {
		if (INSTANCE == null) {
			INSTANCE = new TransportClientSingleton(servers);
		}
		
		return INSTANCE;
	}
	
	public void close() {
		if (client != null) {
			client.close();
		}
	}
	
	public Client getClient() {
		return this.client;
	}
	
}
