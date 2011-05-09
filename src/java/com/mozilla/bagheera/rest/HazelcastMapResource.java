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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.impl.ascii.rest.RestValue;
import com.mozilla.bagheera.util.IdUtil;

/**
 * A REST resource that inserts data into Hazelcast maps.
 */
@Path("/submit")
public class HazelcastMapResource extends ResourceBase {

	private static final Logger LOG = Logger.getLogger(HazelcastMapResource.class);
	
	public HazelcastMapResource() throws IOException {
		super();
	}
	
	/**
	 * A REST POST that generates an id and put the id,data pair into a map with the given name.
	 * @param name
	 * @param request
	 * @return
	 * @throws IOException
	 */
	@POST
	@Path("{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response mapPut(@PathParam("name") String name, @Context HttpServletRequest request) throws IOException {	
		return mapPut(name, new String(IdUtil.generateBucketizedId()), request);
	}
	
	/**
	 * A REST POST that puts the id,data pair into the map with the given name.
	 * @param name
	 * @param id
	 * @param request
	 * @return
	 * @throws IOException
	 */
	@POST
	@Path("{name}/{id}")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response mapPut(@PathParam("name") String name, @PathParam("id") String id, @Context HttpServletRequest request) throws IOException {
		// Read in the JSON data straight from the request
		// TODO: Should we consider using a model or not here?
		BufferedReader reader = new BufferedReader(new InputStreamReader(request.getInputStream()), 8192);
		String line = null;
		StringBuilder sb = new StringBuilder();
		while ((line = reader.readLine()) != null) {
			sb.append(line);
		}

		Map<String,RestValue> m = Hazelcast.getMap(name);
		RestValue rv = new RestValue();
		rv.setValue(Bytes.toBytes(sb.toString()));
		m.put(new String(IdUtil.bucketizeId(id)), rv);
		
		return Response.ok().build();
	}
	
}
