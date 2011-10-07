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

import com.mozilla.bagheera.rest.properties.WildcardProperties;
import com.mozilla.bagheera.rest.stats.Stats;
import com.mozilla.bagheera.rest.validation.Validator;

public class RESTSingleton {
    
    private static RESTSingleton INSTANCE;
    
    private final WildcardProperties props;
    private final Stats stats;
    private final Validator validator;
    
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
        stats = Stats.getInstance();
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
    
    public Stats getStats() {
        return stats;
    }
}
