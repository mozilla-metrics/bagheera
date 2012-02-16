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
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.maxmind.geoip.LookupService;
import com.mozilla.bagheera.rest.stats.Stats;
import com.mozilla.bagheera.rest.validation.Validator;
import com.mozilla.bagheera.util.WildcardProperties;

public class RESTSingleton {
    
    private static final Logger LOG = Logger.getLogger(RESTSingleton.class);
    
    private static RESTSingleton INSTANCE;
    
    private final WildcardProperties props;
    private final Map<String,Stats> statsMap;
    private final Validator validator;
    private final LookupService geoIpLookupService;
    
    private RESTSingleton() {
        props = new WildcardProperties();
        InputStream in = null;
        try {
            in = getClass().getResource(PROPERTIES_RESOURCE_NAME).openStream();
            if (in == null) {
                throw new IllegalArgumentException("Could not find the properites file: " + PROPERTIES_RESOURCE_NAME);
            }
            props.load(in);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        
        validator = new Validator(props);
        statsMap = new HashMap<String,Stats>();
        
        // Initialize GeoIP if needed.
        String maxMindDbPath = props.getProperty("general.maxmind.db.path");
        if (maxMindDbPath != null) {
            geoIpLookupService = initializeGeoIpLookupService(maxMindDbPath);
        } else {
            geoIpLookupService = null;
        }
    }
    
    private LookupService initializeGeoIpLookupService(String maxMindDbPath) {
        LookupService lookupService = null;
        try {
            lookupService = new LookupService(maxMindDbPath, LookupService.GEOIP_MEMORY_CACHE);
        } catch (IOException e) {
            LOG.error("Error initializing GeoIP service", e);
        }
        return lookupService;
    }
    
    public static RESTSingleton getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new RESTSingleton();
        }
        return INSTANCE;
    }
    
    public WildcardProperties getWildcardProperties() {
        return props;
    }
    
    public Validator getValidator() {
        return validator;
    }
    
    public Map<String,Stats> getStatsMap() {
        return statsMap;
    }
    
    public Stats getStats(String mapName) {
        Stats stats = null;
        if (statsMap.containsKey(mapName)) {
            stats = statsMap.get(mapName);
        } else {
            stats = new Stats();
            statsMap.put(mapName, stats);
        }
        
        return stats;
    }
    
    public LookupService getGeoIpLookupService() {
        return geoIpLookupService;
    }
    
}
