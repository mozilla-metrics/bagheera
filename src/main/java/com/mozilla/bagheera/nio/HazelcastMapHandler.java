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
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.CharsetUtil;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.mozilla.bagheera.metrics.MetricsManager;
import com.mozilla.bagheera.nio.codec.http.BagheeraHttpRequest;
import com.mozilla.bagheera.nio.codec.http.HttpSecurityException;
import com.mozilla.bagheera.nio.codec.http.InvalidPathException;
import com.mozilla.bagheera.nio.codec.json.InvalidJsonException;
import com.mozilla.bagheera.util.HttpUtil;

public class HazelcastMapHandler extends SimpleChannelUpstreamHandler {

    private static final Logger LOG = Logger.getLogger(HazelcastMapHandler.class);

    // REST path indices
    public static final int ENDPOINT_PATH_IDX = 0;
    public static final int NAMESPACE_PATH_IDX = 1;
    public static final int ID_PATH_IDX = 2;

    // REST endpoints
    private static final String ENDPOINT_SUBMIT = "submit";
    
    // Specialized REST namespaces
    private static final String NS_METRICS = "metrics";
    
    private HazelcastInstance hzInstance;
    private MetricsProcessor metricsProcessor;
    private MetricsManager metricsManager;
    
    public HazelcastMapHandler(HazelcastInstance hzInstance, MetricsProcessor metricsProcessor) {
        this.hzInstance = hzInstance;
        this.metricsProcessor = metricsProcessor;
        this.metricsManager = MetricsManager.getInstance();
    }
 
    private void updateRequestMetrics(String namespace, String method, int size) {
        metricsManager.getHttpMetricForNamespace(namespace).updateRequestMetrics(method, size);
        metricsManager.getGlobalHttpMetric().updateRequestMetrics(method, size);
    }

    private void updateResponseMetrics(String namespace, int status) {
        if (namespace != null) {
            metricsManager.getHttpMetricForNamespace(namespace).updateResponseMetrics(status);
        }
        metricsManager.getGlobalHttpMetric().updateResponseMetrics(status);
    }
    
    private void handlePost(MessageEvent e, HttpRequest request, String namespace, String id, IMap<String,String> m) {
        HttpResponseStatus status = BAD_REQUEST;
        ChannelBuffer content = request.getContent();

        if (content.readable() && content.readableBytes() > 0) {
            if (NS_METRICS.equals(namespace)) {
                status = metricsProcessor.process(m, id, content.toString(CharsetUtil.UTF_8), 
                                                  e.getChannel().getRemoteAddress().toString(), 
                                                  request.getHeader("X-Obsolete-Document"));
            } else {
                m.putAsync(id, content.toString(CharsetUtil.UTF_8));
                status = CREATED;
            }
        }

        updateRequestMetrics(namespace, request.getMethod().getName(), content.readableBytes());
        writeResponse(status, e, namespace, URI.create(id).toString());
    }
    
    private void handleGet(MessageEvent e, HttpRequest request, String namespace, String id, IMap<String,String> m) {
        String v = m.get(id);
        if (v != null) {
            updateRequestMetrics(namespace, request.getMethod().getName(), v.length());
            writeResponse(OK, e, namespace, m.get(id));
        } else {
            updateRequestMetrics(namespace, request.getMethod().getName(), 0);
            writeResponse(NO_CONTENT, e, namespace, null);
        }
    }
    
    private void handleDelete(MessageEvent e, HttpRequest request, String namespace, String id, IMap<String,String> m) {
        // Hazelcast won't call underlying persistence delete method unless the entry exists
        m.put(id, "");
        String v = m.remove(id);
        if (v != null) {
            updateRequestMetrics(namespace, request.getMethod().getName(), 0);
            writeResponse(OK, e, namespace, null);
        } else {
            updateRequestMetrics(namespace, request.getMethod().getName(), 0);
            writeResponse(NO_CONTENT, e, namespace, null);
        }
    }
    
    private void writeResponse(HttpResponseStatus status, MessageEvent e, String namespace, String entity) {
        // Build the response object.
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
        response.addHeader(CONTENT_TYPE, "plain/text");
        if (entity != null) {
            ChannelBuffer buf = ChannelBuffers.wrappedBuffer(entity.getBytes(CharsetUtil.UTF_8));
            response.setContent(buf);
            response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());
        }

        // Write response
        ChannelFuture future = e.getChannel().write(response);
        future.addListener(ChannelFutureListener.CLOSE);
        
        updateResponseMetrics(namespace, response.getStatus().getCode());
    }
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();

        if (msg instanceof BagheeraHttpRequest) {
            BagheeraHttpRequest request = (BagheeraHttpRequest) e.getMessage();
            if (request.getEndpoint() != null && ENDPOINT_SUBMIT.equals(request.getEndpoint())) {
                IMap<String,String> m = hzInstance.getMap(request.getNamespace());
                if (request.getMethod() == HttpMethod.POST || request.getMethod() == HttpMethod.PUT) {
                    handlePost(e, request, request.getNamespace(), request.getId(), m);
                } else if (request.getMethod() == HttpMethod.GET) {
                    handleGet(e, request, request.getNamespace(), request.getId(), m);
                } else if (request.getMethod() == HttpMethod.DELETE) {
                    handleDelete(e, request, request.getNamespace(), request.getId(), m);
                } else {
                    writeResponse(NOT_FOUND, e, request.getNamespace(), null);
                }
            } else {
                String userAgent = request.getHeader("User-Agent");
                String remoteIpAddress = HttpUtil.getRemoteAddr(request, ((InetSocketAddress)e.getChannel().getRemoteAddress()).getAddress().getHostAddress());
                LOG.warn(String.format("Tried to access invalid resource - \"%s\" \"%s\"", remoteIpAddress, userAgent));
                writeResponse(NOT_FOUND, e, null, null);
            }
        } else {
            writeResponse(INTERNAL_SERVER_ERROR, e, null, null);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        Throwable cause = e.getCause();        
        HttpResponse response = null;
        if (cause instanceof InvalidJsonException) {
            LOG.error(cause.getMessage());
            response = new DefaultHttpResponse(HTTP_1_1, BAD_REQUEST);
        } else if (cause instanceof TooLongFrameException) {
            response = new DefaultHttpResponse(HTTP_1_1, REQUEST_ENTITY_TOO_LARGE);
        } else if (cause instanceof InvalidPathException) {
            response = new DefaultHttpResponse(HTTP_1_1, NOT_FOUND);
        } else if (cause instanceof HttpSecurityException) {
            LOG.error(cause.getMessage());
            response = new DefaultHttpResponse(HTTP_1_1, FORBIDDEN);
        } else {
            LOG.error(cause.getMessage());
            response = new DefaultHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR);
        }
        
        response.addHeader(CONTENT_TYPE, "plain/text");
        ChannelFuture future = e.getChannel().write(response);
        future.addListener(ChannelFutureListener.CLOSE);
        
        updateResponseMetrics(null, response.getStatus().getCode());
    }
    
}
