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

import static com.mozilla.bagheera.nio.BagheeraNio.PROPERTIES_RESOURCE_NAME;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Set;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentDecompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

import com.mozilla.bagheera.nio.codec.http.AccessFilter;
import com.mozilla.bagheera.nio.codec.http.ContentLengthFilter;
import com.mozilla.bagheera.nio.codec.http.RootResponse;
import com.mozilla.bagheera.nio.codec.json.JsonFilter;
import com.mozilla.bagheera.nio.validation.Validator;
import com.mozilla.bagheera.util.WildcardProperties;

public class HttpServerPipelineFactory implements ChannelPipelineFactory {

    private final WildcardProperties props;    
    private final int maxContentLength;
    private final Validator validator;
    private final MetricsProcessor metricsProcessor;
    
    public HttpServerPipelineFactory(Set<String> validNamespaces) throws IOException {
        props = new WildcardProperties();
        InputStream in = null;
        try {
            URL propUrl = getClass().getResource(PROPERTIES_RESOURCE_NAME);
            if (propUrl == null) {
                throw new IllegalArgumentException("Could not find the properites file: " + PROPERTIES_RESOURCE_NAME);
            }
            in = propUrl.openStream();
            props.load(in);
        } finally {
            if (in != null) {
                in.close();
            }
        }
        
        validator = new Validator(validNamespaces);
        metricsProcessor = new MetricsProcessor(props.getProperty("maxmind.db.path"));
        maxContentLength = Integer.parseInt(props.getProperty("max.content.length","1048576"));
    }
    
    /* (non-Javadoc)
     * @see org.jboss.netty.channel.ChannelPipelineFactory#getPipeline()
     */
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        
        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("rootResponse", new RootResponse());
        pipeline.addLast("aggregator", new HttpChunkAggregator(maxContentLength));
        pipeline.addLast("contentLengthFilter", new ContentLengthFilter(maxContentLength));
        pipeline.addLast("accessFilter", new AccessFilter(validator, HazelcastMapHandler.NAMESPACE_PATH_IDX, props));
        pipeline.addLast("inflater", new HttpContentDecompressor());
        pipeline.addLast("jsonValidaton", new JsonFilter(validator));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("handler", new HazelcastMapHandler(metricsProcessor));
        
        return pipeline;
    }

}