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
package com.mozilla.bagheera.rest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import com.sun.jersey.spi.container.servlet.ServletContainer;

public class Bagheera {

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		
		// Call this here to initialize it once
		int hbasePoolSize = Integer.parseInt(System.getProperty("hbase.pool.size", "10"));
		RESTServlet servlet = RESTServlet.getInstance(conf, hbasePoolSize);
		    
		int port = Integer.parseInt(System.getProperty("server.port", "8080"));
		Server server = new Server(port);
		ServletContextHandler root = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
		
		ServletHolder jerseyHolder = new ServletHolder(ServletContainer.class);
		jerseyHolder.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", 
									  ResourceConfig.class.getCanonicalName());
		jerseyHolder.setInitParameter("com.sun.jersey.config.property.packages", "jetty");
		
		root.addServlet(jerseyHolder, "/*");
		
		server.setSendServerVersion(false);
	    server.setSendDateHeader(false);
	    server.setStopAtShutdown(true);
	    
		server.start();
		server.join();
	}
	
}
