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
package com.mozilla.bagheera.hazelcast;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

import com.hazelcast.core.Hazelcast;
import com.mozilla.bagheera.dao.HBaseTableDao;

public class HazelQueuePoller {

	private BlockingQueue<String> queue;
	private HBaseTableDao hbaseDao;
	
	public HazelQueuePoller(String name, HBaseTableDao hbaseDao) {
		this.queue = Hazelcast.getQueue(name);
		this.hbaseDao = hbaseDao;
	}
	
	public void run() {
		try {
			while(true) {
				List<String> values = new ArrayList<String>();
				for (int i=0; i < 100 && queue.size() > 0; i++) {
					String v = queue.poll(5, TimeUnit.SECONDS);
					values.add(v);
				}
				try {
					hbaseDao.putStringList(values);
				} catch (IOException e) {
					// TODO: Log an error
					// Something bad here so put these values back in the queue
					for (String v : values) {
						queue.put(v);
					}
				}
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		String tableName = "metrics_ping";
		int hbasePoolSize = Integer.parseInt(System.getProperty("hbase.pool.size", "10"));
		HTablePool pool = null;
		try {
			pool = new HTablePool(conf, hbasePoolSize);
			HBaseTableDao hbaseDao = new HBaseTableDao(pool, tableName, "ping", "json");
			HazelQueuePoller queuePoller = new HazelQueuePoller(tableName, hbaseDao);
			queuePoller.run();
		} finally {
			if (pool != null) {
				pool.closeTablePool(tableName);
			}
		}
	}
	
}
