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
import java.net.URI;
import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.log4j.Logger;

import com.hazelcast.core.Hazelcast;
import com.mozilla.bagheera.rest.properties.WildcardProperties;
import com.mozilla.bagheera.rest.validation.Validator;

/**
 * A REST resource that inserts data into Hazelcast maps.
 */
@Path("/map")
public class HazelcastMapResource extends ResourceBase {

    private static final Logger LOG = Logger.getLogger(HazelcastMapResource.class);

    // property suffixes
    private static final String ALLOW_GET_ACCESS = ".allow.get.access";
    private static final String ALLOW_DEL_ACCESS = ".allow.delete.access";
    private static final String POST_RESPONSE = ".post.response";
    
    // system independent newline
    public static String NEWLINE = System.getProperty("line.separator");
    
    private Validator validator;
    private WildcardProperties props;
    
    public HazelcastMapResource() {
        super();
        validator = rs.getValidator();
        props = rs.getWildcardProperties();
    }

    /**
     * A REST POST that generates an id and put the id,data pair into a map with
     * the given name.
     * 
     * @param name
     * @param request
     * @return
     * @throws IOException
     */
    @POST
    @Path("{name}")
    @Consumes({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    public Response mapPut(@PathParam("name") String name, @Context HttpServletRequest request) throws IOException {
        return mapPut(name, UUID.randomUUID().toString(), request);
    }

    /**
     * A REST POST that puts the id,data pair into the map with the given name.
     * 
     * @param name
     * @param id
     * @param request
     * @return
     * @throws IOException
     */
    @POST
    @Path("{name}/{id}")
    @Consumes({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    public Response mapPut(@PathParam("name") String name, @PathParam("id") String id,
                           @Context HttpServletRequest request) throws IOException {
        rs.getStats().numRequests.incrementAndGet();
        // Check the map name to make sure it's valid
        if (!validator.isValidMapName(name)) {
            // Get the user-agent and IP address
            String userAgent = request.getHeader("User-Agent");
            String remoteIpAddress = request.getRemoteAddr();
            LOG.warn(String.format("Tried to access invalid map name - \"%s\" \"%s\")", remoteIpAddress, userAgent));
            rs.getStats().numInvalidRequests.incrementAndGet();
            return Response.status(Status.NOT_ACCEPTABLE).build();
        }

        // Check the payload size versus any map specific restrictions
        if (!validator.isValidRequestSize(name, request.getContentLength())) {
            rs.getStats().numInvalidRequests.incrementAndGet();
            return Response.status(Status.NOT_ACCEPTABLE).build();
        }

        // Read in the JSON data straight from the request
        BufferedReader reader = new BufferedReader(new InputStreamReader(request.getInputStream()), 8192);
        StringBuilder sb = new StringBuilder();
        char[] buffer = new char[8192];
        int n;
        while((n = reader.read(buffer)) >= 0) {
            sb.append(buffer, 0, n);
        }

        // Validate JSON (open schema)
        if (!validator.isValidJSON(name, sb.toString())) {
            rs.getStats().numInvalidRequests.incrementAndGet(); 
            return Response.status(Status.NOT_ACCEPTABLE).build();
        }
        
        rs.getStats().numValidRequests.incrementAndGet();
        Map<String, String> m = Hazelcast.getMap(name);
        m.put(id, sb.toString());
        rs.getStats().numPuts.incrementAndGet();

        boolean postResponse = Boolean.parseBoolean(props.getWildcardProperty(name + POST_RESPONSE, "false"));
        if (postResponse) {
            return Response.created(URI.create(id)).build();
        }

        return Response.noContent().build();
    }

    @GET
    @Path("{name}/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response mapGet(@PathParam("name") String name, @PathParam("id") String id) throws IOException {
        boolean allowGetAccess = Boolean.parseBoolean(props.getWildcardProperty(name + ALLOW_GET_ACCESS, "false"));
        if (!allowGetAccess) {
            return Response.status(Status.FORBIDDEN).build();
        }

        Map<String, String> m = Hazelcast.getMap(name);
        // This won't have any fields filled out other than the payload
        String data = m.get(id);
        rs.getStats().numGets.incrementAndGet();
        Response resp = null;
        if (data != null) {
            resp = Response.ok(data, MediaType.APPLICATION_JSON).build();
        } else {
            resp = Response.status(Status.NOT_FOUND).build();
        }

        return resp;
    }
    

    /**
     * A REST DELETE that removes the given key from the map.
     * 
     * @param name
     * @param id
     * @param request
     * @return
     * @throws IOException
     */
    @DELETE
    @Path("{name}/{id}")
    @Consumes({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    public Response mapDel(@PathParam("name") String name, @PathParam("id") String id,
                           @Context HttpServletRequest request) throws IOException {
        boolean allowAccess = Boolean.parseBoolean(props.getWildcardProperty(name + ALLOW_DEL_ACCESS, "false"));
        if (!allowAccess) {
            return Response.status(Status.FORBIDDEN).build();
        }
        
        rs.getStats().numRequests.incrementAndGet();
        
        // Check the map name to make sure it's valid
        if (!validator.isValidMapName(name)) {
            // Get the user-agent and IP address
            String userAgent = request.getHeader("User-Agent");
            String remoteIpAddress = request.getRemoteAddr();
            LOG.warn(String.format("Tried to access invalid map name - \"%s\" \"%s\")", remoteIpAddress, userAgent));
            rs.getStats().numInvalidRequests.incrementAndGet();
            return Response.status(Status.NOT_ACCEPTABLE).build();
        }

        Map<String, String> m = Hazelcast.getMap(name);
        String data = m.remove(id);
        
        rs.getStats().numDels.incrementAndGet();
        Response resp = null;
        if (data != null) {
            resp = Response.ok(id, MediaType.APPLICATION_JSON).build();
        } else {
            LOG.warn(String.format("Delete requested, but no record found for key \"%s\"", id));
            resp = Response.status(Status.NOT_FOUND).build();
        }

        rs.getStats().numValidRequests.incrementAndGet();
        return resp;
    }
    
}
