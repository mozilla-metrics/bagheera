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
package com.mozilla.bagheera.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class WildcardProperties extends Properties {

    private static final long serialVersionUID = 8726833938438520686L;

    private static final String WILDCARD = "*";
    private static final char WILDCARD_CHAR = '*';
    private Set<String> wildcardKeys = new HashSet<String>();
    
    public void load(InputStream is) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line = null;
        while ((line = reader.readLine()) != null) {
            if (!line.startsWith("#") && !line.trim().isEmpty()) {
                String[] splits = line.split("=");
                String k = splits[0];
                if (k.contains(WILDCARD)) {
                    int starIdx = k.indexOf(WILDCARD_CHAR);
                    if (starIdx >= -1) {
                        wildcardKeys.add(k);
                    }
                } 
                put(k, splits[1]);
            }
        }
    }
    
    /**
     * Get a property if it exists. If not check for wildcard property matches.
     * 
     * @param name
     * @return null if property does not exist
     * @return String if property exists
     */
    public String getWildcardProperty(String name) {
        String v = null;
        if (containsKey(name)) {
            v = getProperty(name);
        } else {
            for (String pk : wildcardKeys) {                
                int starIdx = pk.indexOf(WILDCARD_CHAR);
                if (name.startsWith(pk.substring(0, starIdx)) && 
                    name.endsWith(pk.substring(starIdx+1))) {
                    v = getProperty(pk);
                    setProperty(name, v);
                    break;
                }
            }
        }
        
        return v;
    }
    
    /**
     * @param name
     * @param defaultValue
     * @return
     */
    public String getWildcardProperty(String name, String defaultValue) {
        String v = getWildcardProperty(name);
        return v == null ? defaultValue : v;
    }
    
    /**
     * @param name
     * @param defaultValue
     * @return
     */
    public boolean getWildcardProperty(String name, boolean defaultValue) {
        String v = getWildcardProperty(name);
        return v == null ? defaultValue : Boolean.parseBoolean(v);
    }
}
