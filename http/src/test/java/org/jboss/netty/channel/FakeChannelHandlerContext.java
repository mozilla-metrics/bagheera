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
package org.jboss.netty.channel;

import org.jboss.netty.handler.execution.ExecutionHandler;

public class FakeChannelHandlerContext implements ChannelHandlerContext {

    private Channel channel;
    private final ExecutionHandler handler;
    
    public FakeChannelHandlerContext(Channel channel, ExecutionHandler handler) {
        this.channel = channel;
        this.handler = handler;
    }
    
    @Override
    public boolean canHandleDownstream() {
        return true;
    }

    @Override
    public boolean canHandleUpstream() {
        return true;
    }

    @Override
    public Object getAttachment() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Channel getChannel() {
        return channel;
    }

    @Override
    public ChannelHandler getHandler() {
        return handler;
    }

    @Override
    public String getName() {
        return handler.getClass().getName();
    }

    @Override
    public ChannelPipeline getPipeline() {
        return null;
    }

    @Override
    public void sendDownstream(ChannelEvent e) {
        // NOOP
    }

    @Override
    public void sendUpstream(ChannelEvent e) {
        // NOOP
    }

    @Override
    public void setAttachment(Object o) {
        // NOOP
    }

}
