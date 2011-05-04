/*
 * Copyright 2011 Mozilla Foundation
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
package com.mozilla.bagheera.rest.nio;

import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.CharsetUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class HttpRequestHandler extends SimpleChannelUpstreamHandler {

	private HttpRequest request;
	private static final Pattern uriPattern = Pattern.compile("/redis/(.+)/(.+)");
	private static final String VALUE_DELIMITER = "\u0001";
	private JedisPool jedisPool;
	
	public HttpRequestHandler(JedisPool jedisPool) {
		this.jedisPool = jedisPool;
	}
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
			HttpRequest request = this.request = (HttpRequest) e.getMessage();

			Matcher uriMatcher = uriPattern.matcher(request.getUri());
			HttpResponseStatus status = HttpResponseStatus.NOT_FOUND;
			if (uriMatcher.find() && uriMatcher.groupCount() == 2) {
				String listName = uriMatcher.group(1);
				String id = uriMatcher.group(2);
				ChannelBuffer content = request.getContent();
				if (content.readable()) {
					StringBuilder sb = new StringBuilder(id);
					sb.append(VALUE_DELIMITER);
					sb.append(content.toString(CharsetUtil.UTF_8));
					Jedis jedis = jedisPool.getResource();
					try {
						jedis.rpush(listName, sb.toString());
						status = HttpResponseStatus.OK;
					} finally {
						jedisPool.returnResource(jedis);
					}
				}
			}

			writeResponse(status, e);			
	}

	private void writeResponse(HttpResponseStatus status, MessageEvent e) {
		// Decide whether to close the connection or not.
		boolean keepAlive = isKeepAlive(request);

		// Build the response object.
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
		response.addHeader(CONTENT_TYPE, "plain/text");
		
		if (keepAlive) {
			// Add 'Content-Length' header only for a keep-alive connection.
			response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());
		}

		// Write the response.
		ChannelFuture future = e.getChannel().write(response);

		// Close the non-keep-alive connection after the write operation is
		// done.
		if (!keepAlive) {
			future.addListener(ChannelFutureListener.CLOSE);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
		e.getCause().printStackTrace();
		e.getChannel().close();
	}
}
