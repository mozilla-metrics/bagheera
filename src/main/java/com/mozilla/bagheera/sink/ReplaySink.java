/*
 * Copyright 2013 Mozilla Foundation
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
package com.mozilla.bagheera.sink;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.UUID;

import org.apache.log4j.Logger;

// TODO: should we queue up deletes and combine them with "store" requests using the "X-Obsolete-Doc" header?

public class ReplaySink implements Sink, KeyValueSink {

    public static final String KEY_PLACEHOLDER = "%k";

    private static final Logger LOG = Logger.getLogger(ReplaySink.class);

    protected final String destinationPattern;
    protected final String destPrefix;
    protected final String destSuffix;
    protected final boolean useSuffix;
    protected final boolean useKey;
    protected final boolean sample;
    protected final double sampleRate;
    protected final boolean copyKeys;
    protected final boolean replayDeletes;

    public ReplaySink(SinkConfiguration sinkConfiguration) {
        this(sinkConfiguration.getString("replaysink.dest"),
                sinkConfiguration.getString("replaysink.sample"),
                sinkConfiguration.getString("replaysink.keys"),
                sinkConfiguration.getString("replaysink.delete"));
    }

    public ReplaySink(String destinationPattern, String sampleRate, String copyKeys, String replayDeletes) {
        this.destinationPattern = destinationPattern;
        int keyPlaceholderLocation = destinationPattern.indexOf(KEY_PLACEHOLDER);
        boolean tmpUseKey = true;
        if (keyPlaceholderLocation == -1) {
            this.destPrefix = destinationPattern;
            this.destSuffix = "";
            this.useSuffix = false;
            tmpUseKey = false;
        } else if (keyPlaceholderLocation == (destinationPattern.length() - 3)) {
            this.destPrefix = destinationPattern.substring(0, keyPlaceholderLocation);
            this.destSuffix = "";
            this.useSuffix = false;
        } else {
            this.destPrefix = destinationPattern.substring(0, keyPlaceholderLocation);
            this.destSuffix = destinationPattern.substring(keyPlaceholderLocation + 2);
            this.useSuffix = true;
        }
        this.sampleRate = Double.parseDouble(sampleRate);
        this.copyKeys = Boolean.parseBoolean(copyKeys);
        this.replayDeletes = Boolean.parseBoolean(replayDeletes);
        if ("1".equals(sampleRate)) {
            sample = false;
        } else {
            sample = true;
        }

        this.useKey = tmpUseKey;
    }

    public String getDest(String key) {
        if (useKey && useSuffix) {
            return destPrefix + key + destSuffix;
        } else if (useKey) {
            return destPrefix + key;
        } else {
            return destinationPattern;
        }
    }

    @Override
    public void close() {
        // nothing to do.
    }

    @Override
    public void store(byte[] data) throws IOException {
        this.store("", data);
    }

    @Override
    public void store(String key, byte[] data) throws IOException {
        boolean go = true;
        if (this.sample) {
            go = (Math.random() < sampleRate);
        }

        if (go) {
            String newKey = key;
            if (!copyKeys) {
                // Assign new UUIDs
                newKey = UUID.randomUUID().toString();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Attempting to replay key '%s' (using '%s' on dest)", key, newKey));
            }

            // TODO: replay it.
            replay("POST", newKey, data);
        } else {
            LOG.debug("Record skipped due to sampling.");
        }
    }

    // Connect to the specified server and replay the given request
    private void replay(String method, String key, byte[] data) {
        URL url;
        HttpURLConnection connection = null;
        try {
            // Create connection
            url = new URL(getDest(key));
            connection = (HttpURLConnection)url.openConnection();
            connection.setRequestMethod(method);

            connection.setUseCaches (false);
            connection.setDoInput(true);
            connection.setDoOutput(true);

            // Send request (if need be)
            if (data != null && data.length > 0) {
                connection.setRequestProperty("Content-Length", String.valueOf(data.length));
                DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
                wr.write(data);
                wr.flush();
                wr.close();
            }

            // Get Response
            // TODO: do we care about the response?
            InputStream is = connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
            String line;
            StringBuffer response = new StringBuffer();
            while((line = rd.readLine()) != null) {
                response.append(line);
                response.append('\n');
            }
            rd.close();
            LOG.debug(response.toString());
        } catch (IOException e) {
            LOG.error("Error replaying request", e);
        } finally {
            if(connection != null) {
                connection.disconnect();
            }
        }
    }

    @Override
    public void store(String key, byte[] data, long timestamp) throws IOException {
        store(key, data);
    }

    @Override
    public void delete(String key) {
        // Note that this breaks the "copyKeys" contract - it would be quite
        // useless to generate random delete keys.

        // Whether or not we process deletes is controlled by a config setting.
        if (replayDeletes) {
            replay("DELETE", key, null);
        }
    }

}
