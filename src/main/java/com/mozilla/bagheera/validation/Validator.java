/*
 * Copyright 2012 Mozilla Foundation
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
package com.mozilla.bagheera.validation;

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;

public class Validator {

    private static final Logger LOG = Logger.getLogger(Validator.class);
    
    private final Pattern validNamespacePattern;
    private final Pattern validUriPattern;
    private final JsonFactory jsonFactory;
    
    public Validator(final String[] validNamespaces) {
        if (validNamespaces == null || validNamespaces.length == 0) {
            throw new IllegalArgumentException("No valid namespace was specified");
        }
        StringBuilder nsPatternBuilder = new StringBuilder("(");
        StringBuilder uriPatternBuilder = new StringBuilder("/submit/(");
        int i=0, size=validNamespaces.length;
        for (String name : validNamespaces) {
            nsPatternBuilder.append(name.replaceAll("\\*", ".+"));
            uriPatternBuilder.append(name.replaceAll("\\*", ".+"));
            if ((i+1) < size) {
                nsPatternBuilder.append("|");
                uriPatternBuilder.append("|");
            }
            i++;
        }
        nsPatternBuilder.append(")");
        uriPatternBuilder.append(")/*([^/]*)");
        LOG.info("Namespace pattern: " + nsPatternBuilder.toString());
        validNamespacePattern = Pattern.compile(nsPatternBuilder.toString());
        LOG.info("URI pattern: " + uriPatternBuilder.toString());
        validUriPattern = Pattern.compile(uriPatternBuilder.toString());
        
        jsonFactory = new JsonFactory();
    }
    
    public boolean isValidNamespace(String ns) {
        return validNamespacePattern.matcher(ns).find();
    }
    
    public boolean isValidUri(String uri) {
        return validUriPattern.matcher(uri).find();
    }

    public boolean isValidJson(String json) {
        boolean isValid = false;
        JsonParser parser = null;
        try {
            parser = jsonFactory.createJsonParser(json);
            while (parser.nextToken() != null) {
                // noop
            }
            isValid = true;
        } catch (JsonParseException ex) {
            LOG.error("JSON parse error");             
        } catch (IOException e) {
            LOG.error("JSON IO error");
        } finally {
            if (parser != null) {
                try {
                    parser.close();
                } catch (IOException e) {
                    LOG.error("Error closing JSON parser", e);
                }
            }
        }
        
        return isValid;
    }
    
    public boolean isValidId(String id) {
        boolean isValid = false;
        try {
            UUID.fromString(id);
            isValid = true;
        } catch (IllegalArgumentException e) {
            LOG.error("Invalid ID: " + id);
        }
        
        return isValid;
    }
}
