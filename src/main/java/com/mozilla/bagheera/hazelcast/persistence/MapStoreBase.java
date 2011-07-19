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
package com.mozilla.bagheera.hazelcast.persistence;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;

public abstract class MapStoreBase implements MapLoaderLifecycleSupport {

    private static final Logger LOG = Logger.getLogger(MapStoreBase.class);
    
    private static final String ALLOW_LOAD = "hazelcast.allow.load";
    private static final String ALLOW_LOAD_ALL = "hazelcast.allow.load.all";
    private static final String ALLOW_DELETE = "hazelcast.allow.delete";
    
    protected boolean allowLoad = false;
    protected boolean allowLoadAll = false;
    protected boolean allowDelete = false;
    
    protected String mapName;
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.hazelcast.core.MapLoaderLifecycleSupport#init(com.hazelcast.core.
     * HazelcastInstance, java.util.Properties, java.lang.String)
     */
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        this.mapName = mapName;
        this.allowLoad = Boolean.parseBoolean(properties.getProperty(ALLOW_LOAD, "false"));
        this.allowLoadAll = Boolean.parseBoolean(properties.getProperty(ALLOW_LOAD_ALL, "false"));
        this.allowDelete = Boolean.parseBoolean(properties.getProperty(ALLOW_DELETE, "false"));
    }
    
}
