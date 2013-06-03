/*
 * Copyright 2013 Mozilla Foundation
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

import static org.jboss.netty.handler.codec.http.HttpMethod.POST;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import scala.actors.threadpool.Arrays;

import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;
import com.mozilla.bagheera.BagheeraProto.BagheeraMessage.Operation;
import com.mozilla.bagheera.metrics.HttpMetric;
import com.mozilla.bagheera.metrics.MetricsManager;
import com.mozilla.bagheera.producer.Producer;
import com.mozilla.bagheera.util.HttpUtil;
import com.mozilla.bagheera.validation.Validator;


public class BagheeraHttpRequestTest {
    private Validator validator;
    private DummyProducer producer;
    private ChannelGroup channelGroup;
    private MetricsManager manager;
    private Channel mockChannel;
    private ChannelBuffer mockChannelBuffer;
    private ChannelFuture mockFuture;

    @Before
    public void setup() {
        validator = Mockito.mock(Validator.class);
        producer = new DummyProducer() ;

        channelGroup = Mockito.mock(ChannelGroup.class);
        manager = Mockito.mock(MetricsManager.class);
        HttpMetric metric = Mockito.mock(HttpMetric.class);
        Mockito.when(manager.getGlobalHttpMetric()).thenReturn(metric);
        Mockito.when(manager.getHttpMetricForNamespace(Mockito.anyString())).thenReturn(metric);

        mockChannelBuffer = Mockito.mock(ChannelBuffer.class);
        Mockito.when(mockChannelBuffer.readable()).thenReturn(true);
        Mockito.when(mockChannelBuffer.readableBytes()).thenReturn(1);
        Mockito.when(mockChannelBuffer.toByteBuffer()).thenReturn(ByteBuffer.allocate(1));

        mockChannel = Mockito.mock(Channel.class);
        mockFuture = Mockito.mock(ChannelFuture.class);
        Mockito.when(mockChannel.getRemoteAddress()).thenReturn(new InetSocketAddress("localhost", 8080));
        Mockito.when(mockChannel.write(Mockito.any())).thenReturn(mockFuture);

        // We get a NoClassDefFoundError without this.
        if (!new File("./target/classes/com/mozilla/bagheera/BagheeraProto.class").exists()) {
            fail("You must run 'mvn compile' before the tests will run properly from Eclipse");
        }
    }

    private BagheeraHttpRequest getMockMessage(HttpMethod method, String id, boolean includeObsoleteDocHeader, String... deletes) {
        BagheeraHttpRequest message = Mockito.mock(BagheeraHttpRequest.class);
        Mockito.when(message.getHeader(HttpUtil.X_FORWARDED_FOR)).thenReturn(null);
        Mockito.when(message.getEndpoint()).thenReturn(SubmissionHandler.ENDPOINT_SUBMIT);
        Mockito.when(message.getMethod()).thenReturn(method);
        Mockito.when(message.getContent()).thenReturn(mockChannelBuffer);
        Mockito.when(message.getNamespace()).thenReturn("test");
        Mockito.when(message.getId()).thenReturn(id);
        Mockito.when(message.containsHeader(SubmissionHandler.HEADER_OBSOLETE_DOCUMENT)).thenReturn(includeObsoleteDocHeader);
        if (deletes != null && deletes.length > 0) {
            @SuppressWarnings("unchecked")
            List<String> deleteList = Arrays.asList(deletes);
            Mockito.when(message.getHeader(SubmissionHandler.HEADER_OBSOLETE_DOCUMENT)).thenReturn(deleteList.get(0));
            Mockito.when(message.getHeaders(SubmissionHandler.HEADER_OBSOLETE_DOCUMENT)).thenReturn(deleteList);
        }

        return message;
    }

    @Test
    public void testURIParsing() {
        String apiVersion = "1.0";
        String namespace = "bar";
        String endpoint = SubmissionHandler.ENDPOINT_SUBMIT;
        String id = "bogusid";
        String partitions = "part1/part2/part3";

        String uri = String.format("/%s/%s/%s/%s/%s", apiVersion, endpoint, namespace, id, partitions);
        BagheeraHttpRequest req = new BagheeraHttpRequest(HTTP_1_1, POST, uri);
        assertEquals(apiVersion, req.getApiVersion());
        assertEquals(namespace, req.getNamespace());
        assertEquals(endpoint, req.getEndpoint());
        assertEquals(id, req.getId());
        List<String> reqPartitions = req.getPartitions();
        assertEquals("part1", reqPartitions.get(0));
        assertEquals("part2", reqPartitions.get(1));
        assertEquals("part3", reqPartitions.get(2));

        req = new BagheeraHttpRequest(HTTP_1_1, POST, String.format("/%s/%s/%s", endpoint, namespace, id));
        assertEquals(null, req.getApiVersion());
        assertEquals(namespace, req.getNamespace());
        assertEquals(endpoint, req.getEndpoint());
        assertEquals(id, req.getId());
        assertEquals(0, req.getPartitions().size());

        req = new BagheeraHttpRequest(HTTP_1_1, POST, String.format("/%s/%s", endpoint, namespace));
        assertEquals(null, req.getApiVersion());
        assertEquals(namespace, req.getNamespace());
        assertEquals(endpoint, req.getEndpoint());
        // Should have an auto-assigned id:
        String randomId = req.getId();
        assertEquals(UUID.randomUUID().toString().length(), randomId.length());
        assertTrue(randomId.matches("^[0-9a-f-]{36}$"));
        UUID uuid = UUID.fromString(randomId);
        assertTrue(uuid != null);
        assertEquals(0, req.getPartitions().size());

        String[] possibleVersions = new String[]{"no",   "1",  "2",  "3",  "1.2", "9.999", "",    "1.2.3", "123", "123.456", "bogus.bogus"};
        boolean[] areTheyVersions = new boolean[]{false, true, true, true, true,  true,    false, false,   true,  true,      false};

        for (int i = 0; i < possibleVersions.length; i++) {
            assertEquals(areTheyVersions[i], req.isApiVersion(possibleVersions[i]));
        }
    }

    @Test
    public void testPostWithoutDelete() throws Exception {
        testMessage("create-id", false, 0, 1);
    }

    @Test
    public void testPostWithDeleteSingle() throws Exception {
        // Each message should result in 1 delete and 1 create
        testMessage("single-delete-id", true, 1, 1, "deadbeef");
    }

    @Test
    public void testPostWithDeleteMulti() throws Exception {
        // Test a a single X-Obsolete-Doc header with a comma-separated list:
        testMessage("multi-delete-id-comma", true, 2, 1, "deadbeef, livebeef"); // 2 items

        // Test multiple X-Obsolete-Doc headers:
        testMessage("multi-delete-id-list", true, 3, 1, "deadbeef", "livebeef", "morebeef"); // 3 items

        // Test a combination:
        testMessage("multi-delete-id-list", true, 5, 1, "deadbeef", "livebeef, morebeef, potatoes", "oranges"); // 5 items
    }

    // Ensure that one invocation of the message results in the specified number of deletes and creates
    // and that two invocations results in double those numbers.
    private void testMessage(String id, boolean doDelete, int expectedDeletes, int expectedCreates, String... deletes) throws Exception {
        SubmissionHandler handler = new SubmissionHandler(validator, producer, channelGroup, manager);
        ChannelHandlerContext context = Mockito.mock(ChannelHandlerContext.class);
        MessageEvent messageEvent = Mockito.mock(MessageEvent.class);
        Mockito.when(messageEvent.getChannel()).thenReturn(mockChannel);
        BagheeraHttpRequest mockMessage = getMockMessage(HttpMethod.POST, id, doDelete, deletes);
        Mockito.when(messageEvent.getMessage()).thenReturn(mockMessage);

        testTwoInvocations(expectedDeletes, expectedCreates, handler, context,
                messageEvent);

        producer.reset();
    }

    private void testTwoInvocations(int expectedDeletes, int expectedCreates,
            SubmissionHandler handler, ChannelHandlerContext context,
            MessageEvent messageEvent) throws Exception {
        handler.messageReceived(context, messageEvent);
        assertEquals(expectedDeletes, producer.getDeleteCount());
        assertEquals(expectedCreates, producer.getCreateCount());

        handler.messageReceived(context, messageEvent);
        assertEquals(expectedDeletes * 2, producer.getDeleteCount());
        assertEquals(expectedCreates * 2, producer.getCreateCount());
    }

    @Test
    public void testDelete() throws Exception {
        // Test an actual delete (as opposed to a POST message with an X-Obsolete-Doc header).
        SubmissionHandler handler = new SubmissionHandler(validator, producer, channelGroup, manager);
        ChannelHandlerContext context = Mockito.mock(ChannelHandlerContext.class);
        MessageEvent messageEvent = Mockito.mock(MessageEvent.class);
        Mockito.when(messageEvent.getChannel()).thenReturn(mockChannel);
        BagheeraHttpRequest mockMessage = getMockMessage(HttpMethod.DELETE, "delete-id", false);
        Mockito.when(messageEvent.getMessage()).thenReturn(mockMessage);

        testTwoInvocations(1, 0, handler, context, messageEvent);

        producer.reset();
    }

    private int dummyNoSplit(String msg) {
        return msg.length();
    }

    private int dummySplit(String msg) {
        String[] split = msg.split(",");
        return split[0].trim().length();
    }

    @Test
    public void testSplitPerformance() {
        // Ensure that the change to support multiple X-Obsolete-Doc headers
        // does not have a large negative impact on performance.
        int numIterations = 1000000;

        long totalLength = 0;
        long start = new Date().getTime();
        String msg = "foooooooooo";
        for (int i = 0; i < numIterations; i++) {
            totalLength += dummyNoSplit(msg);
        }
        long finish = new Date().getTime();

        long noSplitDuration = finish - start;

        start = new Date().getTime();
        for (int i = 0; i < numIterations; i++) {
            totalLength -= dummySplit(msg);
        }
        finish = new Date().getTime();
        long splitDuration = finish - start;

        assertEquals(0, totalLength);

        long durationDiff = Math.abs(noSplitDuration - splitDuration);

        // Make sure the difference in execution time is less than 0.5ms per iteration.
        double diffPerIteration = (double)durationDiff / numIterations;
        assertTrue(diffPerIteration <= 0.5);

        System.out.println(String.format("Without split, it took %d.  With split, it took %d.  Splitting was %d ms %s (%.05f ms per iteration)",
                noSplitDuration, splitDuration, durationDiff, (noSplitDuration < splitDuration ? "slower" : "faster"), diffPerIteration));
    }
}

class DummyProducer implements Producer {
    private int deleteCount = 0;
    private int createCount = 0;

    @Override
    public void close() throws IOException { }

    public void reset() {
        deleteCount = 0;
        createCount = 0;
    }

    @Override
    public void send(BagheeraMessage msg) {
        if (msg.getOperation() == Operation.CREATE_UPDATE) {
            setCreateCount(getCreateCount() + 1);
        } else if(msg.getOperation() == Operation.DELETE) {
            setDeleteCount(getDeleteCount() + 1);
        }
    }

    public int getDeleteCount() {
        return deleteCount;
    }

    public void setDeleteCount(int deleteCount) {
        this.deleteCount = deleteCount;
    }

    public int getCreateCount() {
        return createCount;
    }

    public void setCreateCount(int createCount) {
        this.createCount = createCount;
    }
}
