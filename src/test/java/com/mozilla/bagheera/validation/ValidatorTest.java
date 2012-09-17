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
package com.mozilla.bagheera.validation;

import static org.junit.Assert.*;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

public class ValidatorTest {

    private Validator validator;
    
    @Before
    public void setup() {
        validator = new Validator(new String[] { "foo", "bar" });
    }
    
    @Test
    public void testIsValidJson() {
        assertTrue(validator.isValidJson("{ \"baz\" : \"blah\" }"));
        assertFalse(validator.isValidJson("{ \"baz : 7 }"));
    }
    
    @Test
    public void testIsValidNamespace() {
        assertTrue(validator.isValidNamespace("foo"));
        assertFalse(validator.isValidNamespace("baz"));
    }
    
    @Test
    public void testIsValidUri() {
        String id = UUID.randomUUID().toString();
        assertTrue(validator.isValidUri("/submit/foo"));
        assertTrue(validator.isValidUri("/submit/foo/" + id));
        assertFalse(validator.isValidUri("/submit/baz"));
        assertFalse(validator.isValidUri("/submit/baz" + id));
    }
    
    public void testIsValidId() {
        String id = UUID.randomUUID().toString();
        assertTrue(validator.isValidId(id));
        assertFalse(validator.isValidId("fakeid"));
    }
}
