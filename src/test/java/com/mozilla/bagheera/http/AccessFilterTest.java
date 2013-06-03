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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.jboss.netty.handler.codec.http.HttpMethod.DELETE;
import static org.jboss.netty.handler.codec.http.HttpMethod.GET;
import static org.jboss.netty.handler.codec.http.HttpMethod.POST;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.UUID;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.DefaultChannelFuture;
import org.jboss.netty.channel.FakeChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.UpstreamMessageEvent;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.junit.Before;
import org.junit.Test;

import com.mozilla.bagheera.util.WildcardProperties;
import com.mozilla.bagheera.validation.Validator;

public class AccessFilterTest {

    private ChannelHandlerContext ctx;
    private InetSocketAddress remoteAddr;
    private AccessFilter filter;

    @Before
    public void setup() throws IOException {
        String[] namespaces = new String[] { "foo_*", "bar" };
        WildcardProperties props = new WildcardProperties();
        String propsFileStr = "foo_*.allow.delete.access=false\n" +
        		              "foo_*.id.validation=true\n" +
        		              "bar.allow.delete.access=true\n" +
        		              "bar.id.validation=false";
        InputStream is = new ByteArrayInputStream(propsFileStr.getBytes("UTF-8"));
        props.load(is);
        filter = new AccessFilter(new Validator(namespaces), props);

        remoteAddr = InetSocketAddress.createUnresolved("192.168.1.1", 51723);

        Channel channel = createMock(Channel.class);
        expect(channel.getCloseFuture()).andReturn(new DefaultChannelFuture(channel, false));
        expect(channel.getRemoteAddress()).andReturn(remoteAddr);

        OrderedMemoryAwareThreadPoolExecutor executor = new OrderedMemoryAwareThreadPoolExecutor(10, 0L, 0L);
        final ExecutionHandler handler = new ExecutionHandler(executor, true, true);

        ctx = new FakeChannelHandlerContext(channel, handler);
    }

    private MessageEvent createMockEvent(Channel channel, HttpVersion protocolVersion, HttpMethod method, String uri) {
        MessageEvent event = createMock(UpstreamMessageEvent.class);
        expect(event.getChannel()).andReturn(channel).anyTimes();
        expect(event.getFuture()).andReturn(new DefaultChannelFuture(channel,false)).anyTimes();
        expect(event.getRemoteAddress()).andReturn(remoteAddr);
        expect(event.getMessage()).andReturn(new BagheeraHttpRequest(protocolVersion, method, uri));
        replay(channel, event);

        return event;
    }

    @Test
    public void testInvalidNamespace() throws Exception {
        boolean success = false;
        try {
            filter.messageReceived(ctx, createMockEvent(ctx.getChannel(), HTTP_1_1, POST, "/bad"));
        } catch (InvalidPathException e) {
            success = true;
        }
        assertTrue(success);
    }

    @Test
    public void testInvalidId() throws Exception {
        boolean success = false;
        try {
            filter.messageReceived(ctx, createMockEvent(ctx.getChannel(), HTTP_1_1, POST, "/submit/foo_blah/fakeid"));
        } catch (InvalidPathException e) {
            success = true;
        }
        assertTrue(success);
    }

    @Test
    public void testGetAccessDenied() throws Exception {
        boolean success = false;
        try {
            filter.messageReceived(ctx, createMockEvent(ctx.getChannel(), HTTP_1_1, GET, "/submit/foo_blah/" + UUID.randomUUID().toString()));
        } catch (HttpSecurityException e) {
            success = true;
        }
        assertTrue(success);
    }

    @Test
    public void testDeleteAccessGranted() throws Exception {
        boolean success = false;
        try {
            filter.messageReceived(ctx, createMockEvent(ctx.getChannel(), HTTP_1_1, DELETE, "/submit/bar/" + UUID.randomUUID().toString()));
            success = true;
        } catch (Exception e) {
        }
        assertTrue(success);
    }

    @Test
    public void testDeleteAccessDenied() throws Exception {
        boolean success = false;
        try {
            filter.messageReceived(ctx, createMockEvent(ctx.getChannel(), HTTP_1_1, DELETE, "/submit/foo_blah/" + UUID.randomUUID().toString()));
        } catch (HttpSecurityException e) {
            success = true;
        }
        assertTrue(success);
    }
}
