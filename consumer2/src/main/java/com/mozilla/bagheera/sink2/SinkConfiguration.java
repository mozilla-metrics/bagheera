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
package com.mozilla.bagheera.sink2;

import java.util.Properties;

public class SinkConfiguration {

    private Properties props;
    
    public SinkConfiguration() {
        this(new Properties());
    }
    
    public SinkConfiguration(Properties props) {
        this.props = props;
    }
    
    public String getString(String key) {
        return props.getProperty(key);
    }
    
    public String getString(String key, String def) {
        return props.containsKey(key) ? props.getProperty(key) : def;
    }
    
    public void setString(String key, String value) {
        if (key == null || value == null) {
            return;
        }
        props.setProperty(key, value);
    }
    
    public int getInt(String key) {
        return Integer.parseInt(props.getProperty(key));
    }
    
    public int getInt(String key, int def) {
        return props.containsKey(key) ? Integer.parseInt(props.getProperty(key)) : def;
    }
    
    public void setInt(String key, int value) {
        props.setProperty(key, String.valueOf(value));
    }
    
    public long getLong(String key) {
        return Long.parseLong(props.getProperty(key));
    }
    
    public long getLong(String key, long def) {
        return props.containsKey(key) ? Long.parseLong(props.getProperty(key)) : def;
    }
    
    public void setLong(String key, long value) {
        props.setProperty(key, String.valueOf(value));
    }
    
    public boolean getBoolean(String key) {
        return Boolean.parseBoolean(props.getProperty(key));
    }
    
    public boolean getBoolean(String key, boolean def) {
        return props.containsKey(key) ? Boolean.parseBoolean(props.getProperty(key)) : def;
    }
    
    public void setBoolean(String key, boolean value) {
        props.setProperty(key, String.valueOf(value));
    }
}
