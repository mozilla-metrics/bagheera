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
package com.mozilla.bagheera.nio.validation;

import java.io.IOException;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;

public class Validator implements NamespaceValidator, UriValidator, JsonValidator {

    private static final Logger LOG = Logger.getLogger(Validator.class);
    
    private final Pattern validNamespacePattern;
    private final Pattern validUriPattern;
    private final JsonFactory jsonFactory;
    
    public Validator(final Set<String> validMapNames) {
        StringBuilder nsPatternBuilder = new StringBuilder("(");
        StringBuilder uriPatternBuilder = new StringBuilder("/(submit|stats)/(");
        int i=0, size=validMapNames.size();
        for (String name : validMapNames) {
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
        validNamespacePattern = Pattern.compile(nsPatternBuilder.toString());
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
    
    
    
}
