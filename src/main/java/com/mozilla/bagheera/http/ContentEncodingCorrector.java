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

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMessage;

/**
 * If Content-Encoding isn't specified this checks the Content-Type to see 
 * if there is any indication that content is compressed even though encoding
 * isn't set.
 */
public class ContentEncodingCorrector extends SimpleChannelUpstreamHandler {

    public static final String MIME_TYPE_JSON_ZLIB = "application/json+zlib";
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();
        if (msg instanceof HttpMessage) {
            HttpMessage m = (HttpMessage) msg;
            String contentEncoding = m.getHeader(HttpHeaders.Names.CONTENT_ENCODING);
            if (contentEncoding == null) {
                String contentType = m.getHeader(HttpHeaders.Names.CONTENT_TYPE);
                if (contentType != null && contentType.startsWith(MIME_TYPE_JSON_ZLIB)) {
                    m.setHeader(HttpHeaders.Names.CONTENT_ENCODING,"deflate");
                }
            }
            Channels.fireMessageReceived(ctx, m, e.getRemoteAddress());
        } else {
            ctx.sendUpstream(e);
        }
    }
    
}
