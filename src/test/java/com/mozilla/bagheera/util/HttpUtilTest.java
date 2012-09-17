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
package com.mozilla.bagheera.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Before;
import org.junit.Test;

public class HttpUtilTest {

    private DefaultHttpRequest request;
    private InetAddress addr;
    
    @Before
    public void setup() {
        request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/foo");
        request.setHeader(HttpUtil.USER_AGENT, "Dummy Agent");
        request.setHeader(HttpUtil.X_FORWARDED_FOR, "127.0.0.2");
        try {
            addr = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        assertNotNull(addr);
    }
    
    @Test
    public void testGetUserAgent() {
        assertEquals("Dummy Agent", HttpUtil.getUserAgent(request));
    }
    
    @Test
    public void testGetRemoteAddrBytes() throws UnknownHostException {
        byte[] addrBytes = HttpUtil.getRemoteAddr(request, addr);
        assert(addrBytes.length == 4);
        assertEquals("127.0.0.2", InetAddress.getByAddress(addrBytes).getHostAddress());
    }
    
    @Test
    public void testGetRemoteAddrString() throws UnknownHostException {
        String addrStr = HttpUtil.getRemoteAddr(request, addr.getHostAddress());
        assertEquals("127.0.0.2", addrStr);
    }
}
