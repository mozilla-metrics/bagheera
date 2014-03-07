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

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.MessageEvent;
import org.junit.Test;
import org.mockito.Mockito;

import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;
import com.mozilla.bagheera.util.HttpUtil;


public class SubmissionHandlerTest {

    @Test
    public void testSetFields() throws Exception {
        SubmissionHandler handler = new SubmissionHandler(null, null, null, null);
        BagheeraMessage.Builder builder = BagheeraMessage.newBuilder();
        BagheeraMessage before = builder.buildPartial();

        assertEquals("", before.getId());
        assertEquals("", before.getNamespace());
        assertEquals("", before.getApiVersion());
        assertEquals(0, before.getPartitionCount());
        assertEquals(0l, before.getTimestamp());

        String expectedNamespace = "test";
        String expectedApiVersion = "2.5";
        String expectedId = "hello there";
        long expectedTimestamp = System.currentTimeMillis();
        List<String> expectedPartitions = new ArrayList<String>();

        BagheeraHttpRequest request = Mockito.mock(BagheeraHttpRequest.class);
        Mockito.when(request.getNamespace()).thenReturn(expectedNamespace);
        Mockito.when(request.getApiVersion()).thenReturn(expectedApiVersion);
        Mockito.when(request.getId()).thenReturn(expectedId);
        Mockito.when(request.getPartitions()).thenReturn(expectedPartitions);

        // Make sure we don't interrogate the mocked InetSocketAddress below.
        Mockito.when(request.getHeader(HttpUtil.X_FORWARDED_FOR)).thenReturn("123.123.123.123");

        MessageEvent event = Mockito.mock(MessageEvent.class);
        Channel channel = Mockito.mock(Channel.class);
        Mockito.when(event.getChannel()).thenReturn(channel);

        InetSocketAddress address = Mockito.mock(InetSocketAddress.class);
        Mockito.when(channel.getRemoteAddress()).thenReturn(address);

        // Do not set the ID
        handler.setMessageFields(request, event, builder, expectedTimestamp, false);

        BagheeraMessage after = builder.build();
        assertEquals("", after.getId()); // <-- missing ID
        assertEquals(expectedNamespace, after.getNamespace());
        assertEquals(expectedApiVersion, after.getApiVersion());
        assertEquals(0, after.getPartitionCount());
        assertEquals(expectedTimestamp, after.getTimestamp());

        builder = BagheeraMessage.newBuilder();
        // This time, *do* set the ID
        handler.setMessageFields(request, event, builder, expectedTimestamp, true);

        after = builder.build();
        assertEquals(expectedId, after.getId()); // <-- ID has been set.
        assertEquals(expectedNamespace, after.getNamespace());
        assertEquals(expectedApiVersion, after.getApiVersion());
        assertEquals(0, after.getPartitionCount());
        assertEquals(expectedTimestamp, after.getTimestamp());

        // Test without specifying an apiVersion
        Mockito.when(request.getApiVersion()).thenReturn(null);
        builder = BagheeraMessage.newBuilder();

        handler.setMessageFields(request, event, builder, expectedTimestamp, true);

        after = builder.build();
        assertEquals(expectedId, after.getId()); // <-- ID has been set.
        assertEquals(expectedNamespace, after.getNamespace());
        assertEquals("", after.getApiVersion());
        assertEquals(0, after.getPartitionCount());
        assertEquals(expectedTimestamp, after.getTimestamp());

        // Test with some partitions
        expectedPartitions.add("hello");
        expectedPartitions.add("goodbye");

        Mockito.when(request.getPartitions()).thenReturn(expectedPartitions);

        builder = BagheeraMessage.newBuilder();

        assertEquals(0, builder.getPartitionCount());
        handler.setMessageFields(request, event, builder, expectedTimestamp, true);
        after = builder.build();
        assertEquals(expectedPartitions.size(), after.getPartitionCount());
        for (int i = 0; i < expectedPartitions.size(); i++) {
            assertEquals(expectedPartitions.get(i), after.getPartition(i));
        }
    }
}
