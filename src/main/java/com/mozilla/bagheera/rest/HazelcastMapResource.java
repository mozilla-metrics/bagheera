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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
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
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.apache.log4j.Logger;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.MapStore;
import com.mozilla.bagheera.hazelcast.persistence.MapStoreRepository;
import com.mozilla.bagheera.rest.interceptors.PreCommitHook;
import com.mozilla.bagheera.rest.stats.Stats;
import com.mozilla.bagheera.rest.validation.Validator;
import com.mozilla.bagheera.util.WildcardProperties;

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
    private static final String PRE_COMMIT = ".pre.commit";
    private static final String PRE_COMMIT_CLASS = ".pre.commit.class";
    
    // system independent newline
    public static String NEWLINE = System.getProperty("line.separator");
    
    // 1 day in milliseconds
    private static final long DAY_IN_MILLIS = 86400000L;
    
    private static final Response NOT_ACCEPTABLE = Response.status(Status.NOT_ACCEPTABLE).header("connection", "close").build();
    private static final Response FORBIDDEN = Response.status(Status.FORBIDDEN).header("connection", "close").build();
    private static final Response SERVER_ERROR = Response.serverError().header("connection", "close").build();
    private static final Response NO_CONTENT = Response.noContent().header("connection", "close").build();
    
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
        Stats stats = rs.getStats(name);
        stats.numRequests.incrementAndGet();
        // Check the map name to make sure it's valid
        if (!validator.isValidMapName(name)) {
            // Get the user-agent and IP address
            String userAgent = request.getHeader("User-Agent");
            String remoteIpAddress = request.getRemoteAddr();
            LOG.warn(String.format("Tried to access invalid map name: %s - \"%s\" \"%s\")", name, remoteIpAddress, userAgent));
            stats.numInvalidRequests.incrementAndGet();
            return NOT_ACCEPTABLE;
        }

        // Check the payload size versus any map specific restrictions
        int contentLength = request.getContentLength();
        if (!validator.isValidRequestSize(name, contentLength)) {
            LOG.warn("Tried to put a value larger than configured maximum for name:" + name);
            stats.numInvalidRequests.incrementAndGet();
            return NOT_ACCEPTABLE;
        }

        // Read and validate in the JSON data straight from the request
        ByteArrayOutputStream baos = new ByteArrayOutputStream(contentLength);
        boolean validateJson = Boolean.parseBoolean(props.getWildcardProperty(name + Validator.VALIDATE_JSON, "true"));
        if (validateJson) {
            // JSON optimization: allow Jackson to do all of the reading/writing
            try {
                validator.validateJSONStream(name, request.getInputStream(), baos);
            } catch (IOException e) {
                LOG.error("Failed to read/validate request");
                stats.numInvalidRequests.incrementAndGet(); 
                return NOT_ACCEPTABLE;
            }
        } else {
            ReadableByteChannel rbc = null;
            WritableByteChannel wbc = null;
            try {
                int bufferSize = contentLength > 32384 ? 32384 : contentLength;
                ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
                rbc = Channels.newChannel(request.getInputStream());
                wbc = Channels.newChannel(baos);
                while (rbc.read(buffer) != -1) {
                    buffer.flip();
                    wbc.write(buffer);
                    buffer.clear();
                }
            } catch (IOException e) {
                LOG.error("Failed to read request");
                stats.numInvalidRequests.incrementAndGet(); 
                return Response.serverError().header("connection", "close").build();
            } finally {
                if (rbc != null) {
                    rbc.close();
                }
                if (wbc != null) {
                    wbc.close();
                }
            }
        }
        String data = baos.toString();
        
        // If this needs to call preCommit do it here
        boolean preCommit = props.getWildcardProperty(name + PRE_COMMIT, false);
        if (preCommit) {
            String preCommitClassName = props.getWildcardProperty(name + PRE_COMMIT_CLASS);
            if (preCommitClassName != null) {
                try {
                    PreCommitHook pch = (PreCommitHook)Class.forName(preCommitClassName).newInstance();
                    data = pch.preCommit(name, id, data, request);
                } catch (Exception e) {
                    LOG.error("Exception caught during preCommit attempt", e);
                    // fail fast and return a 500
                    return SERVER_ERROR;
                }
            }
        }
        
        stats.numValidRequests.incrementAndGet();
        Map<String, String> m = Hazelcast.getMap(name);
        m.put(id, data);
        stats.numPuts.incrementAndGet();
        stats.lastUpdate = System.currentTimeMillis();
        
        boolean postResponse = props.getWildcardProperty(name + POST_RESPONSE, false);
        if (postResponse) {
            return Response.created(URI.create(id)).header("connection", "close").build();
        }

        return NO_CONTENT;
    }

    @GET
    @Path("{name}/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response mapGet(@PathParam("name") String name, @PathParam("id") String id, 
                           @Context HttpServletRequest request) throws IOException {
        Stats stats = rs.getStats(name);
        stats.numRequests.incrementAndGet();
        
        boolean allowGetAccess = Boolean.parseBoolean(props.getWildcardProperty(name + ALLOW_GET_ACCESS, "false"));
        if (!allowGetAccess) {
            stats.numForbiddenRequests.incrementAndGet();
            return FORBIDDEN;
        }

        // Check the map name to make sure it's valid
        if (!validator.isValidMapName(name)) {
            // Get the user-agent and IP address
            String userAgent = request.getHeader("User-Agent");
            String remoteIpAddress = request.getRemoteAddr();
            LOG.warn(String.format("Tried to access invalid map name - \"%s\" \"%s\")", remoteIpAddress, userAgent));
            stats.numInvalidRequests.incrementAndGet();
            return NOT_ACCEPTABLE;
        }
        
        stats.numValidRequests.incrementAndGet();
        Map<String, String> m = Hazelcast.getMap(name);
        String data = m.get(id);
        stats.numGets.incrementAndGet();
        
        Response resp = null;
        if (data != null) {
            resp = Response.ok(data, MediaType.APPLICATION_JSON).header("connection", "close").build();
        } else {
            resp = Response.status(Status.NOT_FOUND).header("connection", "close").build();
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
    public Response mapDel(@PathParam("name") String name, @PathParam("id") String id,
                           @Context HttpServletRequest request) throws IOException {
        Stats stats = rs.getStats(name);
        stats.numRequests.incrementAndGet();
        
        boolean allowAccess = Boolean.parseBoolean(props.getWildcardProperty(name + ALLOW_DEL_ACCESS, "false"));
        if (!allowAccess) {
            stats.numForbiddenRequests.incrementAndGet();
            return FORBIDDEN;
        }

        // Check the map name to make sure it's valid
        if (!validator.isValidMapName(name)) {
            // Get the user-agent and IP address
            String userAgent = request.getHeader("User-Agent");
            String remoteIpAddress = request.getRemoteAddr();
            LOG.warn(String.format("Tried to access invalid map name - \"%s\" \"%s\")", remoteIpAddress, userAgent));
            stats.numInvalidRequests.incrementAndGet();
            return NOT_ACCEPTABLE;
        }

        stats.numValidRequests.incrementAndGet();    
        Map<String, String> m = Hazelcast.getMap(name);
        String data = m.remove(id);
        stats.numDels.incrementAndGet();
        
        Response resp = null;
        if (data != null) {
            resp = Response.ok(id, MediaType.APPLICATION_JSON).header("connection", "close").build();
        } else {
            LOG.warn(String.format("Delete requested, but no record found for key \"%s\"", id));
            resp = Response.status(Status.NOT_FOUND).header("connection", "close").build();
        }
        
        return resp;
    }
    
    /**
     * A RESTful way to do timed conditional closing of the underlying MapStore if it supports it.
     * 
     * @param name
     * @return
     */
    @GET
    @Path("close/{name}")
    public Response mapClose(@PathParam("name") String name) {
        Stats stats = rs.getStats(name);
        ResponseBuilder resp = Response.notModified();
        if (stats.lastUpdate < (System.currentTimeMillis() - DAY_IN_MILLIS)) {
            // Get the backing MapStore implementation and close it if possible
            MapStore<String,String> ms = MapStoreRepository.getMapStore(name);
            if (ms instanceof Closeable) {
                try {
                    ((Closeable) ms).close();
                    resp = Response.ok();
                } catch (IOException e) {
                    LOG.error("Error closing map name: " + name, e);
                }
            }
        }
        
        return resp.header("connection", "close").build();
    }
}
