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

package com.mozilla.bagheera.cli;

import com.mozilla.bagheera.metrics.MetricsManager;
import com.yammer.metrics.HealthChecks;
import com.yammer.metrics.util.DeadlockHealthCheck;

/**
 * Base class for apps with a main method.
 */
public abstract class App {
    private static boolean healthChecksInitialized = false;
    private static boolean metricsInitialized = false;

    public static void initializeApp() {
        prepareHealthChecks();
        prepareMetrics();
    }

    public static synchronized void prepareHealthChecks() {
        if (healthChecksInitialized) {
            return;
        }
        HealthChecks.register(new DeadlockHealthCheck());
        healthChecksInitialized = true;
    }

    public static synchronized void prepareMetrics() {
        if (metricsInitialized) {
            return;
        }

        // Initialize metrics collection and reporting.  While 'manager' is not
        // directly used, this call is necessary to begin reporting metrics.
        @SuppressWarnings("unused")
        final MetricsManager manager = MetricsManager.getDefaultMetricsManager();
        metricsInitialized = true;
    }
}

