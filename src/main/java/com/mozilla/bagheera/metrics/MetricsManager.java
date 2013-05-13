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

import com.yammer.metrics.reporting.ConsoleReporter;
import com.yammer.metrics.reporting.GangliaReporter;
import com.yammer.metrics.reporting.GraphiteReporter;

public class MetricsManager {
    private static final String DEFAULT_METRICS_PROPERTIES_RESOURCE_NAME = "/bagheera.metrics.properties";
    private static final String DEFAULT_METRICS_PROPERTIES_PREFIX = "bagheera.metrics.";
    private static final String GLOBAL_HTTP_METRIC_ID = "global";

    private ConcurrentMap<String, HttpMetric> httpMetrics = new ConcurrentHashMap<String, HttpMetric>();

    public static MetricsManager getDefaultMetricsManager() {
        final Properties properties = readProperties(DEFAULT_METRICS_PROPERTIES_RESOURCE_NAME);
        return new MetricsManager(properties, DEFAULT_METRICS_PROPERTIES_PREFIX);
    }

    /**
     * Each MetricsManager instance currently shares state (Ganglia, Graphite).
     * Alas.
     */
    public MetricsManager(final Properties properties, final String propertiesPrefix) {
        configureReporters(properties, propertiesPrefix);
        HttpMetric h = new HttpMetric(GLOBAL_HTTP_METRIC_ID);
        httpMetrics.put(GLOBAL_HTTP_METRIC_ID, h);
    }

    private static String getProp(final Properties props, final String prefix, final String key) {
        return props.getProperty(prefix + key, null);
    }

    private static boolean getBoolean(final Properties props, final String prefix, final String key) {
        String value = props.getProperty(prefix + key, null);
        if (value == null) {
            return false;
        }
        return Boolean.parseBoolean(value);
    }

    private static long getLong(final Properties props, final String prefix, final String key) {
        String value = props.getProperty(prefix + key, null);
        if (value == null) {
            return 0;
        }
        return Long.parseLong(value);
    }

    private static int getInt(final Properties props, final String prefix, final String key) {
        String value = props.getProperty(prefix + key, null);
        if (value == null) {
            return 0;
        }
        return Integer.parseInt(value);
    }

    private static void configureReporters(final Properties props, final String prefix) {
        if (getBoolean(props, prefix, "ganglia.enable")) {
            GangliaReporter.enable(getLong(props, prefix, "ganglia.update.secs"),
                                   TimeUnit.SECONDS,
                                   getProp(props, prefix, "ganglia.host"),
                                   getInt(props, prefix, "ganglia.port"));
        }
        if (getBoolean(props, prefix, "graphite.enable")) {
            GraphiteReporter.enable(getLong(props, prefix, "graphite.update.secs"),
                                    TimeUnit.SECONDS,
                                    getProp(props, prefix, "graphite.host"),
                                    getInt(props, prefix, "graphite.port"));
        }
        if (getBoolean(props, prefix, "consolereporter.enable")) {
            ConsoleReporter.enable(getLong(props, prefix, "consolereporter.update.secs"),
                                    TimeUnit.SECONDS);
        }
    }

    public HttpMetric getGlobalHttpMetric() {
        return getHttpMetricForNamespace(GLOBAL_HTTP_METRIC_ID);
    }

    public HttpMetric getHttpMetricForNamespace(final String ns) {
        final HttpMetric metric = httpMetrics.get(ns);
        if (metric != null) {
            return metric;
        }

        final HttpMetric newMetric = httpMetrics.putIfAbsent(ns, new HttpMetric(ns));
        if (newMetric == null) {
            return httpMetrics.get(ns);
        }
        return newMetric;
    }

    /**
     * Utility to read a properties file from a path.
     */
    public static Properties readProperties(final String path) {
        final Properties properties = new Properties();
        final InputStream in = MetricsManager.class.getResourceAsStream(path);
        try {
            try {
                properties.load(in);
            } finally {
                in.close();
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not find the properties file: " + path);
        } catch (Exception e) {
            throw new IllegalArgumentException("Exception reading " + path + ": " + e);
        }
        return properties;
    }
}
