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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.HealthChecks;
import com.yammer.metrics.reporting.GangliaReporter;
import com.yammer.metrics.reporting.GraphiteReporter;
import com.yammer.metrics.util.DeadlockHealthCheck;

public class MetricsManager {

    private static final String METRICS_PROPERTIES_RESOURCE_NAME = "/bagheera.metrics.properties";
    private static final String METRICS_PROPERTIES_PREFIX = "bagheera.metrics.";
    private static final String METRICS_NAME_PREFIX = "bagheera";
    private static final String GLOBAL_HTTP_METRIC_ID = METRICS_NAME_PREFIX + "." + "global";

    private static MetricsManager instance = null;

    private final Properties props;
    private ConcurrentMap<String, HttpMetric> httpMetrics;

    private MetricsManager() {
        props = new Properties();
        InputStream in = getClass().getResourceAsStream(METRICS_PROPERTIES_RESOURCE_NAME);
        try {
            props.load(in);
            in.close();
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not find the properites file: " + METRICS_PROPERTIES_RESOURCE_NAME);
        }
        
        configureHealthChecks();
        configureReporters();
        configureHttpMetrics();
    }

    private void configureHealthChecks() {
        HealthChecks.register(new DeadlockHealthCheck());
    }
    
    private void configureReporters() {
        if (Boolean.parseBoolean(getConfigParam("ganglia.enable"))) {
            GangliaReporter.enable(Long.parseLong(getConfigParam("ganglia.update.secs")), TimeUnit.SECONDS,
                    getConfigParam("ganglia.host"), Integer.parseInt(getConfigParam("ganglia.port")));
        }
        if (Boolean.parseBoolean(getConfigParam("graphite.enable"))) {
            GraphiteReporter.enable(Long.parseLong(getConfigParam("graphite.update.secs")), TimeUnit.SECONDS,
                    getConfigParam("graphite.host"), Integer.parseInt(getConfigParam("graphite.port")));
        }
    }

    private void configureHttpMetrics() {
        httpMetrics = new ConcurrentHashMap<String, HttpMetric>();
        HttpMetric h = new HttpMetric(GLOBAL_HTTP_METRIC_ID);
        httpMetrics.put(GLOBAL_HTTP_METRIC_ID, h);
    }
    
    private String namespaceToId(String namespace) {
        return METRICS_NAME_PREFIX + ".ns." + namespace;
    }
    
    private HttpMetric getHttpMetricForId(String id) {
        final HttpMetric metric = httpMetrics.get(id);
        if (metric == null) {
            final HttpMetric newMetric = httpMetrics.putIfAbsent(id, new HttpMetric(id));
            if (newMetric == null) {
                return httpMetrics.get(id);
            } else {
                return newMetric;
            }
        } else {
            return metric;
        }
    }
    
    public synchronized static MetricsManager getInstance() {
        if (instance == null) {
            instance = new MetricsManager();
        }
        return instance;
    }

    public String getConfigParam(String name) {
        return getConfigParam(name, null);
    }

    public String getConfigParam(String name, String defval) {
        return props.getProperty(METRICS_PROPERTIES_PREFIX + name, defval);
    }

    public HttpMetric getGlobalHttpMetric() {
        return getHttpMetricForId(GLOBAL_HTTP_METRIC_ID);
    }    

    public HttpMetric getHttpMetricForNamespace(String namespace) {
        return getHttpMetricForId(namespaceToId(namespace));
    }
}