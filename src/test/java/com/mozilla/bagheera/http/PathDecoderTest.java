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
package com.mozilla.bagheera.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class PathDecoderTest {

    @Test
    public void testPathDecoder() {
        PathDecoder pd = new PathDecoder("/submit/foo/fakeid");
        assertEquals("submit", pd.getPathElement(0));
        assertEquals("foo", pd.getPathElement(1));
        assertEquals("fakeid", pd.getPathElement(2));
        assertNull(pd.getPathElement(3));

        assertEquals(3, pd.size());


        pd = new PathDecoder("/1/2/3/4/5/6/7/8/9/10");
        for (int i = 1; i <= 10; i++) {
            assertEquals(String.valueOf(i), pd.getPathElement(i-1));
        }
        assertEquals(10, pd.size());
    }
}
