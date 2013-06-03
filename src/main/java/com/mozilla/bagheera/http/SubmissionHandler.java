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
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.channels.ClosedChannelException;

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
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.CharsetUtil;

import com.google.protobuf.ByteString;
import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;
import com.mozilla.bagheera.BagheeraProto.BagheeraMessage.Operation;
import com.mozilla.bagheera.metrics.MetricsManager;
import com.mozilla.bagheera.producer.Producer;
import com.mozilla.bagheera.util.HttpUtil;
import com.mozilla.bagheera.validation.Validator;

public class SubmissionHandler extends SimpleChannelUpstreamHandler {

    private static final Logger LOG = Logger.getLogger(SubmissionHandler.class);

    // REST endpoints
    public static final String ENDPOINT_SUBMIT = "submit";

    private final Producer producer;
    private final ChannelGroup channelGroup;
    private final MetricsManager metricsManager;

    public SubmissionHandler(Validator validator,
                             Producer producer,
                             ChannelGroup channelGroup,
                             MetricsManager metricsManager) {
        this.producer = producer;
        this.channelGroup = channelGroup;
        this.metricsManager = metricsManager;
    }

    private void updateRequestMetrics(String namespace, String method, int size) {
        this.metricsManager.getHttpMetricForNamespace(namespace).updateRequestMetrics(method, size);
        this.metricsManager.getGlobalHttpMetric().updateRequestMetrics(method, size);
    }

    private void updateResponseMetrics(String namespace, int status) {
        if (namespace != null) {
            this.metricsManager.getHttpMetricForNamespace(namespace).updateResponseMetrics(status);
        }
        this.metricsManager.getGlobalHttpMetric().updateResponseMetrics(status);
    }

    private void handlePost(MessageEvent e, BagheeraHttpRequest request) {
        HttpResponseStatus status = BAD_REQUEST;
        ChannelBuffer content = request.getContent();

        if (content.readable() && content.readableBytes() > 0) {
            // TODO: insert apiVersion and partition info
            BagheeraMessage.Builder bmsgBuilder = BagheeraMessage.newBuilder();
            bmsgBuilder.setNamespace(request.getNamespace());
            bmsgBuilder.setId(request.getId());
            bmsgBuilder.setIpAddr(ByteString.copyFrom(HttpUtil.getRemoteAddr(request,
                                                                             ((InetSocketAddress)e.getChannel().getRemoteAddress()).getAddress())));
            bmsgBuilder.setPayload(ByteString.copyFrom(content.toByteBuffer()));
            bmsgBuilder.setTimestamp(System.currentTimeMillis());
            producer.send(bmsgBuilder.build());

            if (request.containsHeader("X-Obsolete-Document")) {
                // TODO: insert apiVersion and partition info (unless we don't care about partitions for deletes)
                String obsoleteId = request.getHeader("X-Obsolete-Document");
                BagheeraMessage.Builder obsBuilder = BagheeraMessage.newBuilder();
                obsBuilder.setOperation(Operation.DELETE);
                obsBuilder.setNamespace(request.getNamespace());
                obsBuilder.setId(obsoleteId);
                obsBuilder.setIpAddr(bmsgBuilder.getIpAddr());
                obsBuilder.setTimestamp(bmsgBuilder.getTimestamp());
                producer.send(obsBuilder.build());
            }

            status = CREATED;
        }

        updateRequestMetrics(request.getNamespace(), request.getMethod().getName(), content.readableBytes());
        writeResponse(status, e, request.getNamespace(), URI.create(request.getId()).toString());
    }

    private void handleDelete(MessageEvent e, BagheeraHttpRequest request) {
        // TODO: insert apiVersion and partition info (unless we don't care about partitions for deletes)
        BagheeraMessage.Builder bmsgBuilder = BagheeraMessage.newBuilder();
        bmsgBuilder.setOperation(Operation.DELETE);
        bmsgBuilder.setNamespace(request.getNamespace());
        bmsgBuilder.setId(request.getId());
        bmsgBuilder.setIpAddr(ByteString.copyFrom(HttpUtil.getRemoteAddr(request,
                                                                         ((InetSocketAddress)e.getChannel().getRemoteAddress()).getAddress())));
        bmsgBuilder.setTimestamp(System.currentTimeMillis());
        producer.send(bmsgBuilder.build());
        updateRequestMetrics(request.getNamespace(), request.getMethod().getName(), 0);
        writeResponse(OK, e, request.getNamespace(), null);
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
        this.channelGroup.add(e.getChannel());
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();

        if (msg instanceof BagheeraHttpRequest) {
            BagheeraHttpRequest request = (BagheeraHttpRequest)e.getMessage();
            if (ENDPOINT_SUBMIT.equals(request.getEndpoint())) {
                if ((request.getMethod() == HttpMethod.POST || request.getMethod() == HttpMethod.PUT)) {
                    handlePost(e, request);
                } else if (request.getMethod() == HttpMethod.GET) {
                    writeResponse(METHOD_NOT_ALLOWED, e, request.getNamespace(), null);
                } else if (request.getMethod() == HttpMethod.DELETE) {
                    handleDelete(e, request);
                }
            } else {
                String remoteIpAddress = HttpUtil.getRemoteAddr(request, ((InetSocketAddress)e.getChannel().getRemoteAddress()).getAddress().getHostAddress());
                LOG.warn(String.format("Tried to access invalid resource - \"%s\" \"%s\"", remoteIpAddress, request.getHeader("User-Agent")));
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
        if (cause instanceof ClosedChannelException) {
            // NOOP
        } else if (cause instanceof TooLongFrameException) {
            // The client doesn't get the response even if we write one. There is an open
            // issue in Netty on this here: https://github.com/netty/netty/issues/1007
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

        if (response != null) {
            ChannelFuture future = e.getChannel().write(response);
            future.addListener(ChannelFutureListener.CLOSE);
            updateResponseMetrics(null, response.getStatus().getCode());
        }
    }

}
