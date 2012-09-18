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

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_ACCEPTABLE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
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
import org.jboss.netty.channel.ChannelStateEvent;
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

import com.google.protobuf.ByteString;
import com.mozilla.bagheera.BagheeraProto;
import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;
import com.mozilla.bagheera.http.json.InvalidJsonException;
import com.mozilla.bagheera.metrics.MetricsManager;
import com.mozilla.bagheera.producer.Producer;
import com.mozilla.bagheera.util.HttpUtil;
import com.mozilla.bagheera.validation.Validator;

public class SubmissionHandler extends SimpleChannelUpstreamHandler {

    private static final Logger LOG = Logger.getLogger(SubmissionHandler.class);

    // REST endpoints
    private static final String ENDPOINT_SUBMIT = "submit";

    private MetricsManager metricsManager;
    private Producer producer;
    
    public SubmissionHandler(Validator validator, Producer producer) {
        this.metricsManager = MetricsManager.getInstance();
        this.producer = producer;
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
    
    private void handlePost(MessageEvent e, BagheeraHttpRequest request) {
        HttpResponseStatus status = NOT_ACCEPTABLE;
        ChannelBuffer content = request.getContent();

        if (content.readable() && content.readableBytes() > 0) {
            BagheeraMessage.Builder bmsgBuilder = BagheeraProto.BagheeraMessage.newBuilder();
            bmsgBuilder.setNamespace(request.getNamespace());
            bmsgBuilder.setId(request.getId());
            bmsgBuilder.setIpAddr(ByteString.copyFrom(HttpUtil.getRemoteAddr(request, 
                                                                             ((InetSocketAddress)e.getChannel().getRemoteAddress()).getAddress())));
            bmsgBuilder.setPayload(ByteString.copyFrom(content.toByteBuffer()));
            bmsgBuilder.setTimestamp(System.currentTimeMillis());
            producer.send(bmsgBuilder.build());
            status = CREATED;
        }

        updateRequestMetrics(request.getNamespace(), request.getMethod().getName(), content.readableBytes());
        writeResponse(status, e, request.getNamespace(), URI.create(request.getId()).toString());
    }
    
    private void handleDelete(MessageEvent e, HttpRequest request, String namespace, String id) {
        // TODO: Handle deletes
        writeResponse(OK, e, namespace, null);
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
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) {
        Bagheera.allChannels.add(e.getChannel());
    }
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();

        if (msg instanceof BagheeraHttpRequest) {
            BagheeraHttpRequest request = (BagheeraHttpRequest)e.getMessage();
            if (request.getEndpoint() != null && ENDPOINT_SUBMIT.equals(request.getEndpoint())) {
                if ((request.getMethod() == HttpMethod.POST || request.getMethod() == HttpMethod.PUT)) {
                    if (request.getId() == null) {
                        handlePost(e, request);
                    } else {
                        handlePost(e, request);
                    }      
                } else if (request.getMethod() == HttpMethod.GET) {
                    writeResponse(METHOD_NOT_ALLOWED, e, request.getNamespace(), null);
                } else if (request.getMethod() == HttpMethod.DELETE) {
                    handleDelete(e, request, request.getNamespace(), request.getId());
                }
            } else {
                String userAgent = request.getHeader("User-Agent");
                String remoteIpAddress = HttpUtil.getRemoteAddr(request, ((InetSocketAddress)e.getChannel().getRemoteAddress()).getAddress().getHostAddress());
                LOG.warn(String.format("Tried to access invalid resource - \"%s\" \"%s\"", remoteIpAddress, userAgent));
                writeResponse(NOT_ACCEPTABLE, e, null, null);
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
            response = new DefaultHttpResponse(HTTP_1_1, NOT_ACCEPTABLE);
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
