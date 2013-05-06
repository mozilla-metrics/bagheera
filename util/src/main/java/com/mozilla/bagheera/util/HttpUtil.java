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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.jboss.netty.handler.codec.http.HttpRequest;

public class HttpUtil {

    // header fields
    public static final String USER_AGENT = "User-Agent";
    public static final String X_FORWARDED_FOR = "X-Forwarded-For";
    
    public static String getUserAgent(HttpRequest request) {
        return request.getHeader(USER_AGENT);
    }
    
    private static String getForwardedAddr(String forwardedAddr) {
        if (forwardedAddr == null) {
            return null;
        }
        
        String clientAddr = null;
        int idx = forwardedAddr.indexOf(",");
        if (idx > 0) {
            clientAddr = forwardedAddr.substring(0, idx);
        } else {
            clientAddr = forwardedAddr;
        }
        
        return clientAddr;
    }
    
    public static String getRemoteAddr(HttpRequest request, String channelRemoteAddr) {
        String forwardedAddr = getForwardedAddr(request.getHeader(X_FORWARDED_FOR));
        return forwardedAddr == null ? channelRemoteAddr : forwardedAddr;
    }
    
    public static byte[] getRemoteAddr(HttpRequest request, InetAddress channelRemoteAddr) {
        String forwardedAddr = getForwardedAddr(request.getHeader(X_FORWARDED_FOR));
        byte[] addrBytes = null;
        if (forwardedAddr != null) {
            InetAddress addr;
            try {
                addr = InetAddress.getByName(forwardedAddr);
                addrBytes = addr.getAddress();
            } catch (UnknownHostException e) {
                // going to swallow this for now
            }
        }
        // if we're still null here then use the remote addr bytes
        if (addrBytes == null) {
            addrBytes = channelRemoteAddr.getAddress();
        }
        
        return addrBytes;
    }
    
}
