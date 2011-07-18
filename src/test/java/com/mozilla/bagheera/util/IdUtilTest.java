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
package com.mozilla.bagheera.util;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.UUID;

import org.junit.Test;

public class IdUtilTest {

    @Test
    public void testGenerateBucketizedId() throws IOException {
        byte[] idBytes = IdUtil.generateBucketizedId();
        assertNotNull(idBytes);
    }
    
    @Test
    public void testBucketizeId1() throws IOException {
        boolean caughtException = false;
        try {
            byte[] idBytes = IdUtil.bucketizeId(null);
        } catch (IllegalArgumentException e) {
            caughtException = true;
        }
        assertTrue(caughtException);
    }
    
    @Test
    public void testBucketizeId2() throws IOException {
        UUID uuid = UUID.randomUUID();
        byte[] idBytes = IdUtil.bucketizeId(uuid.toString());
        assertNotNull(idBytes);
        String bucketIdStr = new String(idBytes);
        assert(bucketIdStr.endsWith(uuid.toString()));
    }
    
}
