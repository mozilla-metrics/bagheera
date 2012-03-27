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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public class HttpMetric {

    private final String id;
    
    private Meter requests, throughput;
    private ConcurrentMap<String,Meter> methods;
    private ConcurrentMap<Integer,Counter> responseCodeCounts;
    
    HttpMetric(String id) {
        this.id = id;
        configureMetrics();
    }
    
    private void configureMetrics() {
        requests = Metrics.newMeter(new MetricName(HttpMetric.class, this.id + ".requests"), "requests", TimeUnit.SECONDS);
        throughput = Metrics.newMeter(new MetricName(HttpMetric.class, this.id + ".throughput"), "bytes", TimeUnit.SECONDS);
        methods = new ConcurrentHashMap<String,Meter>();
        responseCodeCounts = new ConcurrentHashMap<Integer,Counter>();
    }

    public void updateRequestMetrics(String method, int contentSize) {
        requests.mark();
        throughput.mark(contentSize);
        if (methods.containsKey(method)) {
            methods.get(method).mark();
        } else {
            Meter methodMeter = Metrics.newMeter(new MetricName(HttpMetric.class, this.id + ".method." + method), "requests", TimeUnit.SECONDS);
            methodMeter.mark();
            methods.put(method, methodMeter);
        }
    }
    
    public void updateResponseMetrics(int status) {
        if (responseCodeCounts.containsKey(status)) {
            responseCodeCounts.get(status).inc();
        } else {
            Counter statusCounter = Metrics.newCounter(new MetricName(HttpMetric.class, this.id + ".response." + status));
            statusCounter.inc();
            responseCodeCounts.put(status, statusCounter);
        }
    }
}
