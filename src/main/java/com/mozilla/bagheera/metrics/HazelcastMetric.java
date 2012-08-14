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
package com.mozilla.bagheera.metrics;

import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public class HazelcastMetric {

    private static final String DEFAULT_GROUP = "bagheera";
    private static final String DEFAULT_TYPE = "hz";
    private final String id;
    
    private Meter loads, stores, deletes;
    private Meter loadFailures, storeFailures, deleteFailures;
    private Meter badKeys;
    
    HazelcastMetric(String id) {
        this.id = id;
        configureMetrics();
    }
    
    private void configureMetrics() {
        loads = Metrics.newMeter(new MetricName(DEFAULT_GROUP, DEFAULT_TYPE, this.id + ".load"), "calls", TimeUnit.SECONDS);
        stores = Metrics.newMeter(new MetricName(DEFAULT_GROUP, DEFAULT_TYPE, this.id + ".store"), "calls", TimeUnit.SECONDS);
        deletes = Metrics.newMeter(new MetricName(DEFAULT_GROUP, DEFAULT_TYPE, this.id + ".delete"), "calls", TimeUnit.SECONDS);
        loadFailures = Metrics.newMeter(new MetricName(DEFAULT_GROUP, DEFAULT_TYPE, this.id + ".load.failed"), "failures", TimeUnit.SECONDS);
        storeFailures = Metrics.newMeter(new MetricName(DEFAULT_GROUP, DEFAULT_TYPE, this.id + ".store.failed"), "failures", TimeUnit.SECONDS);
        deleteFailures = Metrics.newMeter(new MetricName(DEFAULT_GROUP, DEFAULT_TYPE, this.id + ".delete.failed"), "failures", TimeUnit.SECONDS);
        badKeys = Metrics.newMeter(new MetricName(DEFAULT_GROUP, DEFAULT_TYPE, this.id + ".badkey"), "badkeys", TimeUnit.SECONDS);
    }
    
    public void updateLoadMetrics(int numLoaded, boolean success) {
        loads.mark(numLoaded);
        if (!success) {
            loadFailures.mark();
        }
    }
    
    public void updateStoreMetrics(int numStored, boolean success) {
        stores.mark(numStored);
        if (success) {
            storeFailures.mark();
        }
    }
    
    public void updateDeleteMetrics(int numDeleted, boolean success) {
        deletes.mark(numDeleted);
        if (success) {
            deleteFailures.mark();
        }
    }
    
    public void updateBadKeyMetrics(int numBadKeys) {
        badKeys.mark(numBadKeys);
    }
    
}
