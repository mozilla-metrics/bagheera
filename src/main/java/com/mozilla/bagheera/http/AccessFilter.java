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

import java.net.InetSocketAddress;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;

import com.mozilla.bagheera.util.HttpUtil;
import com.mozilla.bagheera.util.WildcardProperties;
import com.mozilla.bagheera.validation.Validator;

public class AccessFilter extends SimpleChannelUpstreamHandler {

    private static final String ALLOW_DELETE_ACCESS = ".allow.delete.access";
    private static final String ID_VALIDATION = ".id.validation";
    
    private final Validator validator;
    private final WildcardProperties props;
    
    public AccessFilter(Validator validator, WildcardProperties props) {
        this.validator = validator;
        this.props = props;
    }    

    private String buildErrorMessage(String msg, HttpRequest request, MessageEvent e) {
        return String.format("%s: %s - \"%s\" \"%s\"", msg, request.getUri(), 
                             HttpUtil.getRemoteAddr(request, ((InetSocketAddress)e.getChannel().getRemoteAddress()).toString()), 
                             request.getHeader(HttpUtil.USER_AGENT));
    }
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();
        if (msg instanceof BagheeraHttpRequest) {
            BagheeraHttpRequest request = (BagheeraHttpRequest)msg;
            // Check Namespace
            if (request.getNamespace() == null || !validator.isValidNamespace(request.getNamespace())) {
                throw new InvalidPathException(buildErrorMessage("Tried to access invalid resource", request, e));
            }
            // Check Id
            boolean validateId = Boolean.parseBoolean(props.getWildcardProperty(request.getNamespace() + ID_VALIDATION, "true"));
            if (request.getId() != null && validateId && !validator.isValidId(request.getId())) {
                throw new InvalidPathException(buildErrorMessage("Submitted an invalid ID", request, e));
            } 
            // Check POST/GET/DELETE Access
            if (request.getMethod() == HttpMethod.POST || request.getMethod() == HttpMethod.PUT) {
                // noop
            } else if (request.getMethod() == HttpMethod.GET) {
                throw new HttpSecurityException(buildErrorMessage("Tried to access GET method for resource", request, e));
            } else if (request.getMethod() == HttpMethod.DELETE) {
                boolean allowDelAccess = Boolean.parseBoolean(props.getWildcardProperty(request.getNamespace() + ALLOW_DELETE_ACCESS, "false"));
                if (!allowDelAccess) {
                    throw new HttpSecurityException(buildErrorMessage("Tried to access DELETE method for resource", request, e));
                }
            } else {
                throw new HttpSecurityException(buildErrorMessage("Tried to access invalid method for resource", request, e));
            }
            Channels.fireMessageReceived(ctx, request, e.getRemoteAddress());
        } else {
            ctx.sendUpstream(e);
        }
    }

}
