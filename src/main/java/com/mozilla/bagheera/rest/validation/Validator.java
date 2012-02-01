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
package com.mozilla.bagheera.rest.validation;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.mozilla.bagheera.rest.properties.WildcardProperties;

public class Validator {
    
    private static final Logger LOG = Logger.getLogger(Validator.class);
    
    // property suffixes
    public static final String MAX_BYTES_POSTFIX = ".max.bytes";
    public static final String ALLOW_GET_ACCESS = ".allow.get.access";
    public static final String VALIDATE_JSON = ".validate.json";
    
    private final WildcardProperties props;
    private final Pattern validMapNames;
    
    private final JsonFactory jsonFactory;
    
    public Validator(WildcardProperties props) {
        this.jsonFactory = new JsonFactory();
        this.props = props;
        
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
        this.validMapNames = Pattern.compile(sb.toString());
    }
    
    /**
     * Validate JSON content
     * 
     * @param content
     * @return
     * @throws IOException
     */
    public boolean isValidJSON(String mapName, String content) throws IOException {
        boolean validateJson = Boolean.parseBoolean(props.getWildcardProperty(mapName + VALIDATE_JSON, "true"));
        boolean isValid = false;
        if (validateJson) {
            // Validate JSON (open schema)
            JsonParser parser = null;
            try {
                parser = jsonFactory.createJsonParser(content);
                while (parser.nextToken() != null) {
                    // noop
                }
                isValid = true;
            } catch (JsonParseException e) {
                // if this was hit we'll return below
                LOG.error("Error parsing JSON");
            } finally {
                if (parser != null) {
                    parser.close();
                }
            }
        }

        return (!validateJson || isValid);
    }
    
    /**
     * Validate a given map name
     * 
     * @param mapName
     * @return
     */
    public boolean isValidMapName(String mapName) {
        return validMapNames.matcher(mapName).find();
    }
        
    /**
     * Validate the content size
     * 
     * @param mapName
     * @param contentLength
     * @return
     */
    public boolean isValidRequestSize(String mapName, int contentLength) {
        int maxByteSize = Integer.parseInt(props.getWildcardProperty(mapName + MAX_BYTES_POSTFIX, "0"));    
        return (maxByteSize == 0 || contentLength <= maxByteSize);
    }
    
}
