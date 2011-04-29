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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.mozilla.bagheera.util.IdUtil;

@Path("/ping")
public class PingResource extends ResourceBase {

	private static final Logger LOG = Logger.getLogger(PingResource.class);
	
	private HTablePool pool;
	private final byte[] family;
	private final byte[] qualifier;
	
	public PingResource() throws IOException {
		super();
		pool = servlet.getTablePool();
		family = Bytes.toBytes("ping");
		qualifier = Bytes.toBytes("json");
	}
	
	@POST
	@Path("{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response ping(@PathParam("name") String name, @Context HttpServletRequest request) throws IOException {
		servlet.getMetrics().incrementRequests(1);
		// Get the specified table
		HTableInterface table = pool.getTable(name);
		if (table == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Table name was not found: name=" + name);
			}
			return Response.status(Response.Status.NOT_FOUND).build();
		}
		
		// Read in the JSON data straight from the request
		// TODO: Should we consider using a model or not here?
		BufferedReader reader = new BufferedReader(new InputStreamReader(request.getInputStream()), 8192);
		String line = null;
		StringBuilder sb = new StringBuilder();
		while ((line = reader.readLine()) != null) {
			sb.append(line);
		}
		
		// Put the value into HBase
		Put p = new Put(IdUtil.generateBucketizedId());
		p.add(family, qualifier, Bytes.toBytes(sb.toString()));
		table.put(p);
		
		return Response.ok().build();
	}
	
	@POST
	@Path("{name}/{id}")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response ping(@PathParam("name") String name, @PathParam("id") String id, @Context HttpServletRequest request) throws IOException {
		servlet.getMetrics().incrementRequests(1);
		// Get the specified table
		HTableInterface table = pool.getTable(name);
		if (table == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Table name was not found: name=" + name);
			}
			return Response.status(Response.Status.NOT_FOUND).build();
		}
		
		// Read in the JSON data straight from the request
		// TODO: Should we consider using a model or not here?
		BufferedReader reader = new BufferedReader(new InputStreamReader(request.getInputStream()), 8192);
		String line = null;
		StringBuilder sb = new StringBuilder();
		while ((line = reader.readLine()) != null) {
			sb.append(line);
		}
		
		// Put the value into HBase
		Put p = new Put(IdUtil.bucketizeId(id));
		p.add(family, qualifier, Bytes.toBytes(sb.toString()));
		table.put(p);
		
		return Response.ok().build();
	}
	
}
