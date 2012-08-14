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
package com.mozilla.bagheera.nio;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.CharsetUtil;

import com.yammer.metrics.HealthChecks;
import com.yammer.metrics.core.HealthCheck.Result;

public class HealthCheckHandler extends SimpleChannelUpstreamHandler {

    private static final String HEALTH_PATH = "/health";
     
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();
        if (msg instanceof HttpRequest) {
            HttpRequest request = (HttpRequest) msg;
            if (HEALTH_PATH.equals(request.getUri()) || request.getUri().isEmpty()) {
                Map<String,Result> healthChecks = HealthChecks.runHealthChecks();
                StringBuilder sb = new StringBuilder("{");
                int i = 0, size = healthChecks.size();
                for (Map.Entry<String,Result> entry : healthChecks.entrySet()) {
                    String k = entry.getKey();
                    Result r = entry.getValue();
                    if (!"deadlocks".equals(k) && r.getMessage() != null) {
                        sb.append("\"").append(k).append("\":{");
                        sb.append(r.getMessage());
                        sb.append("}");
                        if ((i+1) < size) {
                            sb.append(",");
                        }
                    }   
                    i++;
                }
                sb.append("}");
                
                writeResponse(OK, e, sb.toString());
            } else {
                Channels.fireMessageReceived(ctx, request, e.getRemoteAddress());
            }
        } else {
            ctx.sendUpstream(e);
        }
    }
    
    private void writeResponse(HttpResponseStatus status, MessageEvent e, String body) {
        // Build the response object.
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
        response.addHeader(CONTENT_TYPE, "application/json");
        if (body != null) {
            ChannelBuffer buf = ChannelBuffers.wrappedBuffer(body.getBytes(CharsetUtil.UTF_8));
            response.setContent(buf);
            response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());
        }

        // Write response
        ChannelFuture future = e.getChannel().write(response);
        future.addListener(ChannelFutureListener.CLOSE);
    }
    
}