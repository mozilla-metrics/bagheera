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
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
    private final Set<String> unvalidatedNamespaces;
    private final Set<String> deletableNamespaces;

    public AccessFilter(Validator validator, WildcardProperties props) {
        this.validator = validator;

        // Populate a cache of the two kinds of permission in props:
        // namespaces for which we *don't* validate IDs, and namespaces
        // for which we *do* allow deletion.
        // Non-membership in these sets implies the negation.
        HashSet<String> unvalidated = new HashSet<String>();
        HashSet<String> deletable = new HashSet<String>();

        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            final String name = (String) entry.getKey();
            if (name.endsWith(ALLOW_DELETE_ACCESS)) {
                final boolean allow = Boolean.parseBoolean((String) entry.getValue());
                if (allow) {
                    final String namespace = name.substring(0, name.lastIndexOf(ALLOW_DELETE_ACCESS));
                    deletable.add(namespace);
                }
            } else if (name.endsWith(ID_VALIDATION)) {
                final boolean allow = Boolean.parseBoolean((String) entry.getValue());
                if (!allow) {
                    final String namespace = name.substring(0, name.lastIndexOf(ID_VALIDATION));
                    unvalidated.add(namespace);
                }
            }
        }

        // Ensure that these cannot be modified.
        unvalidatedNamespaces = Collections.unmodifiableSet(unvalidated);
        deletableNamespaces = Collections.unmodifiableSet(deletable);
    }    

    private static String buildErrorMessage(String msg, HttpRequest request, MessageEvent e) {
        return String.format("%s: %s - \"%s\" \"%s\"", msg, request.getUri(), 
                             HttpUtil.getRemoteAddr(request, ((InetSocketAddress)e.getChannel().getRemoteAddress()).toString()), 
                             request.getHeader(HttpUtil.USER_AGENT));
    }

    protected boolean isDeletableNamespace(final String namespace) {
        return this.deletableNamespaces.contains(namespace);
    }

    protected boolean isValidatedNamespace(final String namespace) {
        return !this.unvalidatedNamespaces.contains(namespace);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        final Object msg = e.getMessage();
        if (!(msg instanceof BagheeraHttpRequest)) {
            ctx.sendUpstream(e);
            return;
        }

        final BagheeraHttpRequest request = (BagheeraHttpRequest) msg;
        final String namespace = request.getNamespace();

        // Check namespace.
        if (namespace == null || !validator.isValidNamespace(namespace)) {
            throw new InvalidPathException(buildErrorMessage("Tried to access invalid resource", request, e));
        }

        // Check id.
        final boolean validateId = this.isValidatedNamespace(namespace);
        final String requestId = request.getId();
        if (requestId != null &&
            validateId &&
            !validator.isValidId(requestId)) {
            throw new InvalidPathException(buildErrorMessage("Submitted an invalid ID", request, e));
        }

        // Check POST/GET/DELETE access.
        final HttpMethod method = request.getMethod();
        if (method == HttpMethod.POST ||
            method == HttpMethod.PUT) {
            // noop
        } else if (method == HttpMethod.GET) {
            throw new HttpSecurityException(buildErrorMessage("Tried to access GET method for resource", request, e));
        } else if (method == HttpMethod.DELETE) {
            if (!this.isDeletableNamespace(namespace)) {
                throw new HttpSecurityException(buildErrorMessage("Tried to access DELETE method for resource", request, e));
            }
        } else {
            throw new HttpSecurityException(buildErrorMessage("Tried to access invalid method for resource", request, e));
        }

        Channels.fireMessageReceived(ctx, request, e.getRemoteAddress());
    }
}
