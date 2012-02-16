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
package com.mozilla.bagheera.nio.codec.http;

import java.net.InetSocketAddress;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;

import com.mozilla.bagheera.nio.validation.NamespaceValidator;
import com.mozilla.bagheera.util.WildcardProperties;


public class AccessFilter extends SimpleChannelUpstreamHandler {

    private static final String ALLOW_GET_ACCESS = ".allow.get.access";
    private static final String ALLOW_DELETE_ACCESS = ".allow.delete.access";
    
    private final NamespaceValidator nsValidator;
    private final int nsPathIdx;
    private final WildcardProperties props;
    
    public AccessFilter(NamespaceValidator nsValidator, int nsPathIdx, WildcardProperties props) {
        this.nsValidator = nsValidator;
        this.nsPathIdx = nsPathIdx;
        this.props = props;
    }
    
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();
        if (msg instanceof HttpRequest) {
            HttpRequest request = (HttpRequest) msg;
            PathDecoder rpd = new PathDecoder(request.getUri());
            String ns = rpd.getPathElement(nsPathIdx);
            if (ns == null || !nsValidator.isValidNamespace(ns)) {
                String userAgent = request.getHeader("User-Agent");
                String remoteIpAddress = ((InetSocketAddress)e.getChannel().getRemoteAddress()).getAddress().getHostAddress();
                throw new SecurityException(String.format("Tried to access invalid resource: %s - \"%s\" \"%s\"", ns, remoteIpAddress, userAgent));
            }
            if (request.getMethod() == HttpMethod.GET) {
                boolean allowGetAccess = Boolean.parseBoolean(props.getWildcardProperty(ns + ALLOW_GET_ACCESS, "false"));
                if (!allowGetAccess) {
                    String userAgent = request.getHeader("User-Agent");
                    String remoteIpAddress = ((InetSocketAddress)e.getChannel().getRemoteAddress()).getAddress().getHostAddress();
                    throw new SecurityException(String.format("Tried to access GET method for resource: %s - \"%s\" \"%s\"", ns, remoteIpAddress, userAgent));
                }
            } else if (request.getMethod() == HttpMethod.DELETE) {
                boolean allowDelAccess = Boolean.parseBoolean(props.getWildcardProperty(ns + ALLOW_DELETE_ACCESS, "false"));
                if (!allowDelAccess) {
                    String userAgent = request.getHeader("User-Agent");
                    String remoteIpAddress = ((InetSocketAddress)e.getChannel().getRemoteAddress()).getAddress().getHostAddress();
                    throw new SecurityException(String.format("Tried to access DELETE method for resource %s - \"%s\" \"%s\"", ns, remoteIpAddress, userAgent));
                }
            }
            Channels.fireMessageReceived(ctx, request, e.getRemoteAddress());
        } else {
            ctx.sendUpstream(e);
        }
    }

}
