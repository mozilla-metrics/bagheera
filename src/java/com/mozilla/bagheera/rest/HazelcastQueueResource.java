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

import static com.mozilla.bagheera.rest.Bagheera.PROPERTIES_RESOURCE_NAME;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.hazelcast.core.Hazelcast;
import com.sun.jersey.api.client.ClientResponse.Status;

/**
 * A REST resource that inserts data into Hazelcast maps.
 */
@Path("/queue")
public class HazelcastQueueResource extends ResourceBase {

	private static final Logger LOG = Logger.getLogger(HazelcastQueueResource.class);

	private Properties props;

	public HazelcastQueueResource() throws IOException {
		super();
		props = new Properties();
		InputStream in = null;
		try {
			in = getClass().getResource(PROPERTIES_RESOURCE_NAME).openStream();
			if (in == null) {
				throw new IllegalArgumentException("Could not find the properites file: " + PROPERTIES_RESOURCE_NAME);
			}
			props.load(in);
		} finally {
			if (in != null) {
				in.close();
			}
		}
	}

	/**
	 * A REST POST that puts the id,data pair into the map with the given name.
	 * 
	 * @param queue
	 * @param name
	 * @param id
	 * @param request
	 * @return
	 * @throws IOException
	 */
	@POST
	@Path("{name}/{id}")
	//@Consumes(MediaType.APPLICATION_JSON)
	public Response mapPut(@PathParam("name") String name, @PathParam("id") String id, @Context HttpServletRequest request) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("HTTP request params, queue name: " + name + "\tid: " + id);
		}

		boolean success = Hazelcast.getQueue(name).add(id);
		LOG.info("Queue size: " + Hazelcast.getQueue(name).size());
		Response response = null;
		if (success) {
			response = Response.status(Status.OK).build();
		} else {
			LOG.error("error inserting elements: queueName: " + name + "\tid: " + id);
			response = Response.status(Status.SERVICE_UNAVAILABLE).build();
		}
		
		return response;
	}

}
