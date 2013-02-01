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
package com.mozilla.bagheera.consumer.validation;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;

public class JsonValidator implements Validator {
    
    private static final Logger LOG = Logger.getLogger(JsonValidator.class);
    
    private final JsonFactory jsonFactory = new JsonFactory();
    
    @Override
    public boolean isValid(byte[] data) {
        boolean isValid = false;
        JsonParser parser = null;
        try {
            parser = jsonFactory.createJsonParser(data);
            while (parser.nextToken() != null) {
                // noop
            }
            isValid = true;
        } catch (JsonParseException ex) {
            LOG.error("JSON parse error");
        } catch (IOException e) {
            LOG.error("JSON IO error");
        } finally {
            if (parser != null) {
                try {
                    parser.close();
                } catch (IOException e) {
                    LOG.error("Error closing JSON parser", e);
                }
            }
        }
        
        return isValid;
    }

}
