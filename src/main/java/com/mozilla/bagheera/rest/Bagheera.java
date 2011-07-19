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

import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.mozilla.bagheera.elasticsearch.ClientFactory;
import com.sun.jersey.spi.container.servlet.ServletContainer;

/**
 * The main server class.
 */
public class Bagheera {

    public static final String PROPERTIES_RESOURCE_NAME = "/bagheera.properties";
  
	public static void main(String[] args) throws Exception {		
		int port = Integer.parseInt(System.getProperty("server.port", "8080"));
		Server server = new Server(port);
		ServletContextHandler root = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
		
		ServletHolder jerseyHolder = new ServletHolder(ServletContainer.class);
		jerseyHolder.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", 
									  ResourceConfig.class.getCanonicalName());
		jerseyHolder.setInitParameter("com.sun.jersey.config.property.packages", "jetty");
		// Init jersey on startup
		jerseyHolder.setInitOrder(1);
		root.addServlet(jerseyHolder, "/*");
		
		server.setSendServerVersion(false);
	    server.setSendDateHeader(false);
	    server.setStopAtShutdown(true);

	    // Initialize Hazelcast now rather than waiting for the first request
		Hazelcast.getDefaultInstance();
		
		boolean initElasticSearch = Boolean.parseBoolean(System.getProperty("init.elasticsearch.onstartup", "true"));
		if (initElasticSearch) {
		    Config config = Hazelcast.getConfig();
		    for (Map.Entry<String, MapConfig> entry : config.getMapConfigs().entrySet()) {
		        MapConfig mapConfig = entry.getValue();
		        if (mapConfig != null) {
		            MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
		            if (mapStoreConfig != null) {
        		        Properties props = mapStoreConfig.getProperties();
        		        Enumeration<?> propEnum = props.propertyNames();
        		        while (propEnum.hasMoreElements()) {
        		            String propKey = (String)propEnum.nextElement();
        		            if (propKey.contains("hazelcast.elasticsearch")) {
        		                ClientFactory.getInstance().getNodeClient(entry.getKey(), props, true);
        		                break;
        		            }
        		        }
		            }
		        }
		    }
		}
		
		server.start();
		server.join();
	}
	
}
