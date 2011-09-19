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
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

public class IdUtilTest {

    Set<String> hexChars = new HashSet<String>();

    @Before
    public void setup() {
        for (int i=0; i < 16; i++) {
            hexChars.add(Integer.toHexString(i));
        }
    }

    @Test
    public void testGenerateBucketizedId() throws IOException {
        byte[] idBytes = IdUtil.generateBucketizedId();
        assertNotNull(idBytes);
    }

    @Test
    public void testBucketizeId1() throws IOException {
        boolean caughtException = false;
        try {
            IdUtil.bucketizeId(null);
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

    @Test
    public void testRandByteBucketizeId1() throws IOException {
        boolean caughtException = false;
        try {
            IdUtil.randByteBucketizeId(null, Calendar.getInstance().getTime());
        } catch (IllegalArgumentException e) {
            caughtException = true;
        }
        assertTrue(caughtException);
    }

    @Test
    public void testRandByteBucketizeId2() throws IOException {
        boolean caughtException = false;
        try {
            UUID uuid = UUID.randomUUID();
            IdUtil.randByteBucketizeId(uuid.toString(), null);
        } catch (IllegalArgumentException e) {
            caughtException = true;
        }
        assertTrue(caughtException);
    }

    @Test
    public void testRandByteBucketizeId3() throws IOException {
        UUID uuid = UUID.randomUUID();
        Date d = Calendar.getInstance().getTime();
        byte[] idBytes = IdUtil.randByteBucketizeId(uuid.toString(), d);
        assertNotNull(idBytes);
        String bucketIdStr = new String(idBytes);
        assert(bucketIdStr.endsWith(IdUtil.SDF.format(d) + uuid.toString()));
    }

    @Test
    public void testRandByteBucketizeId4() throws IOException {
        UUID uuid = UUID.randomUUID();
        Date d = Calendar.getInstance().getTime();
        byte[] idBytes1 = IdUtil.randByteBucketizeId(uuid.toString(), d);
        assertNotNull(idBytes1);
        String bucketIdStr1 = new String(idBytes1);

        byte[] idBytes2 = IdUtil.randByteBucketizeId(uuid.toString(), d);
        assertNotNull(idBytes2);
        String bucketIdStr2 = new String(idBytes2);

        assertFalse(bucketIdStr1.equals(bucketIdStr2));
    }

    @Test
    public void testNonRandByteBucketizeId1() throws IOException {
        boolean caughtException = false;
        try {
            IdUtil.nonRandByteBucketizeId(null, Calendar.getInstance().getTime());
        } catch (IllegalArgumentException e) {
            caughtException = true;
        }
        assertTrue(caughtException);
    }

    @Test
    public void testNonRandByteBucketizeId2() throws IOException {
        boolean caughtException = false;
        try {
            UUID uuid = UUID.randomUUID();
            IdUtil.nonRandByteBucketizeId(uuid.toString(), null);
        } catch (IllegalArgumentException e) {
            caughtException = true;
        }
        assertTrue(caughtException);
    }

    @Test
    public void testNonRandByteBucketizeId3() throws IOException {
        UUID uuid = UUID.randomUUID();
        Date d = Calendar.getInstance().getTime();
        byte[] idBytes = IdUtil.nonRandByteBucketizeId(uuid.toString(), d);
        assertNotNull(idBytes);
        String bucketIdStr = new String(idBytes);
        assert(bucketIdStr.endsWith(IdUtil.SDF.format(d) + uuid.toString()));
    }

    @Test
    public void testNonRandByteBucketizeId4() throws IOException {
        UUID uuid = UUID.randomUUID();
        Date d = Calendar.getInstance().getTime();
        byte[] idBytes1 = IdUtil.nonRandByteBucketizeId(uuid.toString(), d);
        assertNotNull(idBytes1);
        String bucketIdStr1 = new String(idBytes1);

        byte[] idBytes2 = IdUtil.nonRandByteBucketizeId(uuid.toString(), d);
        assertNotNull(idBytes2);
        String bucketIdStr2 = new String(idBytes2);

        assertTrue(bucketIdStr1.equals(bucketIdStr2));
    }

    @Test
    public void testRandHexBucketizeId1() throws IOException {
        boolean caughtException = false;
        try {
            IdUtil.randHexBucketizeId(null, Calendar.getInstance().getTime());
        } catch (IllegalArgumentException e) {
            caughtException = true;
        }
        assertTrue(caughtException);
    }

    @Test
    public void testRandHexBucketizeId2() throws IOException {
        boolean caughtException = false;
        try {
            UUID uuid = UUID.randomUUID();
            IdUtil.randHexBucketizeId(uuid.toString(), null);
        } catch (IllegalArgumentException e) {
            caughtException = true;
        }
        assertTrue(caughtException);
    }

    @Test
    public void testRandHexBucketizeId3() throws IOException {
        UUID uuid = UUID.randomUUID();
        Date d = Calendar.getInstance().getTime();
        byte[] idBytes = IdUtil.randHexBucketizeId(uuid.toString(), d);
        assertNotNull(idBytes);
        String bucketIdStr = new String(idBytes);
        assert(bucketIdStr.endsWith(IdUtil.SDF.format(d) + uuid.toString()));
        assertTrue(hexChars.contains(bucketIdStr.substring(0,1)));
    }

    @Test
    public void testRandHexBucketizeId4() throws IOException {
        UUID uuid = UUID.randomUUID();
        Date d = Calendar.getInstance().getTime();
        byte[] idBytes1 = IdUtil.randHexBucketizeId(uuid.toString(), d);
        assertNotNull(idBytes1);
        String bucketIdStr1 = new String(idBytes1);

        byte[] idBytes2 = IdUtil.randHexBucketizeId(uuid.toString(), d);
        assertNotNull(idBytes2);
        String bucketIdStr2 = new String(idBytes2);

        assertFalse(bucketIdStr1.equals(bucketIdStr2));
        assertTrue(hexChars.contains(bucketIdStr1.substring(0,1)));
        assertTrue(hexChars.contains(bucketIdStr2.substring(0,1)));
    }
}
