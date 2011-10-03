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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
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
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;

/**
 * A REST resource that inserts data into Hazelcast maps.
 */
@Path("/map")
public class HazelcastMapResource extends ResourceBase {

    private static final Logger LOG = Logger.getLogger(HazelcastMapResource.class);

    private static final String MAX_BYTES_POSTFIX = ".max.bytes";
    private static final String ALLOW_GET_ACCESS = ".allow.get.access";
    private static final String POST_RESPONSE = ".post.response";
    private static final String VALIDATE_JSON = ".validate.json";
    
    // system independent newline
    public static String NEWLINE = System.getProperty("line.separator");
    
    private final JsonFactory jsonFactory;
    private Properties props;

    private Pattern validMapNames;
    private Pattern propertyPattern;
    
    public HazelcastMapResource() throws IOException {
        super();
        jsonFactory = new JsonFactory();
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

        // Construct a regular expression to validate map names
        Map<String, MapConfig> mapConfigs = Hazelcast.getConfig().getMapConfigs();
        StringBuilder sb = new StringBuilder("(");
        for (String wc : mapConfigs.keySet()) {
            if (wc.contains("*")) {
                sb.append(wc.replaceAll("\\*", ".+"));
            } else {
                sb.append(wc);
            }
            sb.append("|");
        }
        if (sb.charAt(sb.length() - 1) == '|') {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(")");
        validMapNames = Pattern.compile(sb.toString());
        propertyPattern = Pattern.compile("^([^\\.]+)\\.(.*)");
    }

    /**
     * Validate a given map name
     * 
     * @param mapName
     * @return
     */
    private boolean validMapName(String mapName) {
        return validMapNames.matcher(mapName).find();
    }

    /**
     * @param propertyName
     * @return
     */
    private String getProperty(String propertyName, String defaultValue) {
        String v = null;
        if (props.containsKey(propertyName)) {
            v = props.getProperty(propertyName);
        } else {
            for (Object k : props.keySet()) {
                String ks = (String)k;
                Matcher m = propertyPattern.matcher(ks);
                if (m.find() && m.groupCount() == 2) {
                    String propMapName = m.group(1);
                    if (propMapName.contains("*")) {
                        Pattern propPattern = Pattern.compile(ks.replaceAll("\\*", ".+"));
                        Matcher m2 = propPattern.matcher(propertyName);
                        if (m2.find()) {
                            v = props.getProperty(ks);
                            break;
                        }
                    }
                }
            }
        }
        
        return v == null ? defaultValue : v;
    }
    
    /**
     * Validate JSON content
     * 
     * @param content
     * @return
     * @throws IOException
     */
    private boolean validJSON(String content) throws IOException {
        // Validate JSON (open schema)
        JsonParser parser = null;
        boolean isValid = false;
        try {
            parser = jsonFactory.createJsonParser(content);
            while (parser.nextToken() != null) {
                // noop
            }
            isValid = true;
        } catch (JsonParseException e) {
            // if this was hit we'll return below
            LOG.error("Error parsing JSON", e);
        }

        return isValid;
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
        // Check the map name to make sure it's valid
        if (!validMapName(name)) {
            // Get the user-agent and IP address
            String userAgent = request.getHeader("User-Agent");
            String remoteIpAddress = request.getRemoteAddr();
            LOG.warn(String.format("Tried to access invalid map name - \"%s\" \"%s\")", remoteIpAddress, userAgent));
            return Response.status(Status.NOT_ACCEPTABLE).build();
        }

        // Check the payload size versus any map specific restrictions
        int maxByteSize = Integer.parseInt(getProperty(name + MAX_BYTES_POSTFIX, "0"));
        if (maxByteSize > 0 && request.getContentLength() > maxByteSize) {
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
        boolean validateJson = Boolean.parseBoolean(getProperty(name + VALIDATE_JSON, "true"));
        if (validateJson && !validJSON(sb.toString())) {
            return Response.status(Status.NOT_ACCEPTABLE).build();
        }
        
        Map<String, String> m = Hazelcast.getMap(name);
        m.put(id, sb.toString());

        boolean postResponse = Boolean.parseBoolean(getProperty(name + POST_RESPONSE, "false"));
        if (postResponse) {
            return Response.created(URI.create(id)).build();
        }

        return Response.noContent().build();
    }

    @GET
    @Path("{name}/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response mapGet(@PathParam("name") String name, @PathParam("id") String id) throws IOException {
        boolean allowGetAccess = Boolean.parseBoolean(props.getProperty(name + ALLOW_GET_ACCESS, "false"));
        if (!allowGetAccess) {
            return Response.status(Status.FORBIDDEN).build();
        }

        Map<String, String> m = Hazelcast.getMap(name);
        // This won't have any fields filled out other than the payload
        String data = m.get(id);
        Response resp = null;
        if (data != null) {
            resp = Response.ok(data, MediaType.APPLICATION_JSON).build();
        } else {
            resp = Response.status(Status.NOT_FOUND).build();
        }

        return resp;
    }
    
}
