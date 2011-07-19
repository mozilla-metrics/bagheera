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

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;

/**
 * A CompositeMapStore consists of two MapStore implementations specified via Hazelcast config. This consists of 
 * setting a MapStore to load data and a second to store data. Currently this doesn't support using the same type
 * of MapStore for both. Typically we use this to load data from HBase and index it with ElasticSearch.
 * 
 * Example:
 * <map-store enabled="true">
 *  <class-name>com.mozilla.bagheera.hazelcast.persistence.CompositeMapStore</class-name>
 *  <property name="hazelcast.composite.load.class.name">com.mozilla.bagheera.hazelcast.persistence.HBaseMapStore</property>
 *  <property name="hazelcast.composite.store.class.name">com.mozilla.bagheera.hazelcast.persistence.ElasticSearchMapStore</property>
 *  ...
 * </map-store>
 */
public class CompositeMapStore extends MapStoreBase implements MapStore<String, String>, MapLoaderLifecycleSupport {

    private static final Logger LOG = Logger.getLogger(CompositeMapStore.class);
    
    private static final String LOAD_MAP_STORE = "hazelcast.composite.load.class.name";
    private static final String STORE_MAP_STORE = "hazelcast.composite.store.class.name";
    
    private MapStore<String,String> loadInstance;
    private boolean loadLifeCycleSupport = false;
    private MapStore<String,String> storeInstance;
    private boolean storeLifeCycleSupport = false;
    
    @SuppressWarnings("unchecked")
    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        super.init(hazelcastInstance, properties, mapName);
        String loadClassName = properties.getProperty(LOAD_MAP_STORE);
        String storeClassName = properties.getProperty(STORE_MAP_STORE);
        try {
            Class<?> loadClass = Class.forName(loadClassName);
            if (loadClass == null) {
                throw new ClassNotFoundException("Load class could not be found");
            }
            loadInstance = (MapStore<String,String>)loadClass.newInstance();
            if (loadInstance instanceof MapLoaderLifecycleSupport) {
                loadLifeCycleSupport = true;
                MapLoaderLifecycleSupport mapLoadLifecycle = (MapLoaderLifecycleSupport)loadInstance;
                mapLoadLifecycle.init(hazelcastInstance, properties, mapName);
            }
            
            Class<?> storeClass = Class.forName(storeClassName);
            if (storeClass == null) {
                throw new ClassNotFoundException("Store class could not be found");
            }
            storeInstance = (MapStore<String,String>)storeClass.newInstance();
            
            if (storeInstance instanceof MapLoaderLifecycleSupport) {
                storeLifeCycleSupport = true;
                MapLoaderLifecycleSupport mapLoadLifecycle = (MapLoaderLifecycleSupport)storeInstance;
                mapLoadLifecycle.init(hazelcastInstance, properties, mapName);
            }
        } catch (InstantiationException e) {
            LOG.error("Failed to instantiate one of the classes required for composite store", e);
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            LOG.error("Illegal access during required setup for composite store", e);
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            LOG.error("Class not found which is required for composite store", e);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void destroy() {
        if (loadLifeCycleSupport) {
            MapLoaderLifecycleSupport mapLoadLifecycle = (MapLoaderLifecycleSupport)loadInstance;
            mapLoadLifecycle.destroy();
        }
        if (storeLifeCycleSupport) {
            MapLoaderLifecycleSupport mapLoadLifecycle = (MapLoaderLifecycleSupport)storeInstance;
            mapLoadLifecycle.destroy();
        }
    }
    
    @Override
    public String load(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, String> loadAll(Collection<String> keys) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<String> loadAllKeys() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void delete(String key) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void store(String key, String value) {
        String loaderValue = loadInstance.load(key);
        if (loaderValue != null) {
            storeInstance.store(key, loaderValue);
        }
    }

    @Override
    public void storeAll(Map<String, String> pairs) {
        Map<String,String> loaderPairs = loadInstance.loadAll(pairs.keySet());
        if (loaderPairs != null) {
            storeInstance.storeAll(loaderPairs);
        }
    }

}
