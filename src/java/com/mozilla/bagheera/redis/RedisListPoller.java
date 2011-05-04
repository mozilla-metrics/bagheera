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
package com.mozilla.bagheera.redis;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

import redis.clients.jedis.Jedis;

import com.mozilla.bagheera.dao.HBaseTableDao;

public class RedisListPoller {

	private static final int DEFAULT_HBASE_BATCH_PUT_SIZE = 100;
	private static final int DEFAULT_REDIS_TIMEOUT = 5000;
	private static final int DEFAULT_SLEEP_TIMEOUT = 5000;
	private static final String VALUE_DELIMITER = "\u0001";
	private static final Pattern VALUE_DELIMITER_PATTERN = Pattern.compile(VALUE_DELIMITER);
	
	private HBaseTableDao hbaseDao;
	private Jedis jedis;
	private String listName;
	
	public RedisListPoller(String redisHostName, String listName, HBaseTableDao hbaseDao) {
		this.jedis = new Jedis(redisHostName);
		this.listName = listName;
		this.hbaseDao = hbaseDao;
	}
	
	public void run() throws IOException {
		try {
			while(true) {
				Map<String,String> values = new HashMap<String,String>();
				long redis_size = jedis.llen(this.listName);
				long batch_size = redis_size > DEFAULT_HBASE_BATCH_PUT_SIZE ? DEFAULT_HBASE_BATCH_PUT_SIZE : redis_size;
				for (long i=0; i < batch_size; i++) {
					List<String> results = jedis.blpop(DEFAULT_REDIS_TIMEOUT, this.listName);
					if (results != null && results.size() > 0) {
						String[] splits = VALUE_DELIMITER_PATTERN.split(results.get(1));
						values.put(splits[0], splits[1]);
					}
				}
				
				if (values.size() > 0) {
					try {
						hbaseDao.putStringMap(values);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						// Something bad here so put these values back in the queue
						for (Map.Entry<String, String> v : values.entrySet()) {
							jedis.lpush(this.listName, v.getKey() + VALUE_DELIMITER + v.getValue());
						}
						
						throw e;
					}
				} else {
					Thread.sleep(DEFAULT_SLEEP_TIMEOUT);
				}
				
				System.out.println("metrics_ping list size: " + jedis.llen(this.listName));
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws IOException {
		String redisHostName = "localhost";
		Configuration conf = HBaseConfiguration.create();
		String tableName = "metrics_ping";
		int hbasePoolSize = Integer.parseInt(System.getProperty("hbase.pool.size", "10"));
		HTablePool pool = null;
		try {
			pool = new HTablePool(conf, hbasePoolSize);
			HBaseTableDao hbaseDao = new HBaseTableDao(pool, tableName, "ping", "json");
			RedisListPoller queuePoller = new RedisListPoller(redisHostName, tableName, hbaseDao);
			queuePoller.run();
		} finally {
			if (pool != null) {
				pool.closeTablePool(tableName);
			}
		}
	}
	
}
