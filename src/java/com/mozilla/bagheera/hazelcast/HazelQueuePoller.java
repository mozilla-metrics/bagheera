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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

import com.hazelcast.core.Hazelcast;
import com.mozilla.bagheera.dao.HBaseTableDao;

public class HazelQueuePoller {

	private static final int DEFAULT_HBASE_BATCH_PUT_SIZE = 100;
	private static final int DEFAULT_SLEEP_TIMEOUT = 5000;
	private static final String VALUE_DELIMITER = "\u0001";
	private static final Pattern VALUE_DELIMITER_PATTERN = Pattern.compile(VALUE_DELIMITER);
	private BlockingQueue<String> queue;
	private HBaseTableDao hbaseDao;
	
	public HazelQueuePoller(String name, HBaseTableDao hbaseDao) {
		this.queue = Hazelcast.getQueue(name);
		this.hbaseDao = hbaseDao;
	}
	
	public void run() throws IOException {		
		try {
			while(true) {
				Map<String,String> values = new HashMap<String,String>();
				long qsize = queue.size();
				long batch_size = qsize > DEFAULT_HBASE_BATCH_PUT_SIZE ? DEFAULT_HBASE_BATCH_PUT_SIZE : qsize;
				for (long i=0; i < batch_size; i++) {
					String item = queue.poll();
					if (item == null) {
						break;
					}
					
					String[] splits = VALUE_DELIMITER_PATTERN.split(item);
					values.put(splits[0], splits[1]);
				}
				
				if (values.size() > 0) {
					try {
						hbaseDao.putStringMap(values);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						// Something bad here so put these values back in the queue
						for (Map.Entry<String, String> v : values.entrySet()) {
							this.queue.put(v.getKey() + VALUE_DELIMITER + v.getValue());
						}
						
						throw e;
					}
				} else {
					Thread.sleep(DEFAULT_SLEEP_TIMEOUT);
				}
				
				System.out.println("metrics_ping list size: " + queue.size());
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws IOException {
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
