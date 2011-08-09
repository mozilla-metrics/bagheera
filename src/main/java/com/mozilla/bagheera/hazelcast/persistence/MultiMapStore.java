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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;

/**
 * A MultiMapStore consists of any number of MapStore implementations specified via Hazelcast config. This consists of
 * setting configuration properties that specify the MapStore implementation to use that begin with MULTI_MAP_STORE_PREFIX.
 *
 * The first store (suffix ".1") is considered to be authoritative, and is used for loads.
 *
 * Example:
 * <map-store enabled="true">
 *  <class-name>com.mozilla.bagheera.hazelcast.persistence.MultiMapStore</class-name>
 *  <property name="hazelcast.multi.store.class.name.1">com.mozilla.bagheera.hazelcast.persistence.HBaseMapStore</property>
 *  <property name="hazelcast.multi.store.class.name.2">com.mozilla.bagheera.hazelcast.persistence.ElasticSearchMapStore</property>
 *  ...
 * </map-store>
 */
public class MultiMapStore extends MapStoreBase implements MapStore<String, String> {

    private static final Logger LOG = Logger.getLogger(MultiMapStore.class);

    private static final String MULTI_MAP_STORE_PREFIX = "hazelcast.multi.store.class.name";

    private List<MapStore<String,String>> mapStores;
    private MapStore<String, String> primaryStore;

    @SuppressWarnings("unchecked")
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        super.init(hazelcastInstance, properties, mapName);

        mapStores = new ArrayList<MapStore<String,String>>();
        for (Map.Entry<Object, Object> prop : properties.entrySet()) {
            Object ko = prop.getKey();
            if (ko instanceof String) {
                String k = (String)ko;
                if (k.startsWith(MULTI_MAP_STORE_PREFIX)) {
                    Object co = prop.getValue();
                    if (co instanceof String) {
                        String className = (String)co;
                        try {
                            MapStore<String,String> classInstance = (MapStore<String,String>)Class.forName(className).newInstance();
                            if (classInstance instanceof MapLoaderLifecycleSupport) {
                                ((MapLoaderLifecycleSupport) classInstance).init(hazelcastInstance, properties, mapName);
                            }
                            if (k.endsWith(".1")) {
                                primaryStore = classInstance;
                            }
                            mapStores.add(classInstance);
                        } catch (InstantiationException e) {
                            LOG.error("Failed to instantiate one of the classes for multi map store", e);
                            throw new RuntimeException(e);
                        } catch (IllegalAccessException e) {
                            LOG.error("Illegal access during required setup for multi map store", e);
                            throw new RuntimeException(e);
                        } catch (ClassNotFoundException e) {
                            LOG.error("Class not found which is required for multi map store", e);
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void destroy() {
        for (MapStore<String,String> ms : mapStores) {
            if (ms instanceof MapLoaderLifecycleSupport) {
                ((MapLoaderLifecycleSupport) ms).destroy();
            }
        }
    }

    @Override
    public String load(String key) {
        return primaryStore.load(key);
    }

    @Override
    public Map<String, String> loadAll(Collection<String> keys) {
        return primaryStore.loadAll(keys);
    }

    @Override
    public Set<String> loadAllKeys() {
        return primaryStore.loadAllKeys();
    }

    @Override
    public void delete(String key) {
        for (MapStore<String, String> ms : mapStores) {
            ms.delete(key);
        }
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        for (MapStore<String, String> ms : mapStores) {
            ms.deleteAll(keys);
        }
    }

    @Override
    public void store(String key, String value) {
        for (MapStore<String,String> ms : mapStores) {
            ms.store(key, value);
        }
    }

    @Override
    public void storeAll(Map<String, String> pairs) {
        for (MapStore<String,String> ms : mapStores) {
            ms.storeAll(pairs);
        }
    }

}
