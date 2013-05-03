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
package com.mozilla.bagheera.sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class HBaseSinkTest {
    SinkConfiguration sinkConfig;
    KeyValueSinkFactory sinkFactory;
    HTablePool hbasePool;
    HTable htable;

    @Before
    public void setup() throws IOException {
        sinkConfig = new SinkConfiguration();
        sinkConfig.setString("hbasesink.hbase.tablename", "test");
        sinkConfig.setString("hbasesink.hbase.column.family", "data");
        sinkConfig.setString("hbasesink.hbase.column.qualifier", "json");
        sinkConfig.setBoolean("hbasesink.hbase.rowkey.prefixdate", false);
        sinkFactory = KeyValueSinkFactory.getInstance(HBaseSink.class, sinkConfig);

        hbasePool = Mockito.mock(HTablePool.class);
        htable = Mockito.mock(HTable.class);

        Mockito.when(hbasePool.getTable("test".getBytes())).thenReturn(htable);

        Mockito.doAnswer(new Answer<Object>() {
            int count = 0;
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                count++;
                // Force code to retry once.
                if (count <= 1) {
                    throw new RetriesExhaustedWithDetailsException(new ArrayList<Throwable>(), new ArrayList<Row>(), new ArrayList<HServerAddress>());
                }
                return null;
            }
        }).when(htable).put(Mockito.anyListOf(Put.class));
    }

    @Test
    public void testRetry() throws ParseException, IOException {
        HBaseSink sink = (HBaseSink) sinkFactory.getSink("test");
        sink.setRetrySleepSeconds(1);
        sink.hbasePool = hbasePool;
        sink.flush();
        Mockito.verify(htable, Mockito.times(1)).clearRegionCache();
    }

    @Test
    public void testLargePut() throws IOException {
        HBaseSink sink = (HBaseSink) sinkFactory.getSink("test");

        @SuppressWarnings("unchecked")
        ConcurrentLinkedQueue<Put> putsQueue = Mockito.mock(ConcurrentLinkedQueue.class);

        sink.putsQueue = putsQueue;
        byte[] theArray = new byte[50*1000*1000]; // 50MB
        for (int i = 0; i < theArray.length; i++) {
            theArray[i] = (byte)(i % 256 - 128);
        }

        sink.store("test1", theArray);
        Mockito.verify(putsQueue, Mockito.times(0)).add((Put)Mockito.any());
        sink.store("test2", "acceptable".getBytes());
        Mockito.verify(putsQueue, Mockito.times(1)).add((Put)Mockito.any());
        sink.store("test3", theArray);
        Mockito.verify(putsQueue, Mockito.times(1)).add((Put)Mockito.any());

        sink.store("test4", theArray, new Date().getTime());
        Mockito.verify(putsQueue, Mockito.times(1)).add((Put)Mockito.any());
        sink.store("test5", "acceptable".getBytes(), new Date().getTime());
        Mockito.verify(putsQueue, Mockito.times(2)).add((Put)Mockito.any());
        sink.store("test6", theArray, new Date().getTime());
        Mockito.verify(putsQueue, Mockito.times(2)).add((Put)Mockito.any());
    }

}
