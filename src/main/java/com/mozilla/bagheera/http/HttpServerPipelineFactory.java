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

import java.io.IOException;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentDecompressor;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

import com.mozilla.bagheera.producer.Producer;
import com.mozilla.bagheera.util.WildcardProperties;
import com.mozilla.bagheera.validation.Validator;

public class HttpServerPipelineFactory implements ChannelPipelineFactory {
    
    private final WildcardProperties props;    
    private final int maxContentLength;
    private final Validator validator;
    private final Producer producer;
    private final ChannelGroup channelGroup;
    
    public HttpServerPipelineFactory(WildcardProperties props, Producer producer, ChannelGroup channelGroup) throws IOException {
        this.props = props;
        String validNsStr = props.getProperty("valid.namespaces");
        if (validNsStr == null || validNsStr.length() == 0) {
            throw new IllegalArgumentException("No valid.namespaces in properties");
        }
        this.validator = new Validator(validNsStr.split(","));
        this.maxContentLength = Integer.parseInt(props.getProperty("max.content.length","1048576"));
        this.producer = producer;
        this.channelGroup = channelGroup;
    }
    
    /* (non-Javadoc)
     * @see org.jboss.netty.channel.ChannelPipelineFactory#getPipeline()
     */
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        
        pipeline.addLast("decoder", new BagheeraHttpRequestDecoder());
        pipeline.addLast("aggregator", new HttpChunkAggregator(maxContentLength));
        pipeline.addLast("contentLengthFilter", new ContentLengthFilter(maxContentLength));
        pipeline.addLast("rootResponse", new RootResponse());
        pipeline.addLast("accessFilter", new AccessFilter(validator, props));
        pipeline.addLast("encodingCorrector", new ContentEncodingCorrector());
        pipeline.addLast("inflater", new HttpContentDecompressor());
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("handler", new SubmissionHandler(validator, producer, this.channelGroup));
        
        return pipeline;
    }

}
