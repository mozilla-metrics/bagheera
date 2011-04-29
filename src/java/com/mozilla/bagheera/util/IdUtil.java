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
package com.mozilla.bagheera.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.hbase.util.Bytes;

public class IdUtil {

	public static final int DEFAULT_NUM_BUCKETS = Byte.MAX_VALUE;
	public static final int HEX_BUCKETS = 16;

	public static final SimpleDateFormat SDF = new SimpleDateFormat("yyyyMMdd");
	private static final Random RAND = new Random();

	public static byte[] generateBucketizedId() throws IOException {
		return byteBucketizeId(UUID.randomUUID().toString(), Calendar.getInstance().getTime());
	}

	public static byte[] bucketizeId(String id) throws IOException {
		return byteBucketizeId(id, Calendar.getInstance().getTime());
	}

	/**
	 * Takes a given id and prefixes it with a byte character and the date
	 * 
	 * @param id
	 * @param d
	 * @return
	 * @throws IOException
	 */
	public static byte[] byteBucketizeId(String id, Date d) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		byte[] randByte = new byte[1];
		RAND.nextBytes(randByte);
		baos.write(randByte);
		baos.write(Bytes.toBytes(SDF.format(d)));
		baos.write(Bytes.toBytes(id));

		return baos.toByteArray();
	}

	/**
	 * Takes a given id and prefixes it with a hex character and the date
	 * 
	 * @param id
	 * @param d
	 * @return
	 * @throws IOException
	 */
	public static byte[] hexBucketizeId(String id, Date d) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		int bucket = RAND.nextInt(HEX_BUCKETS);
		baos.write(Bytes.toBytes(Integer.toHexString(bucket)));
		baos.write(Bytes.toBytes(SDF.format(d)));
		baos.write(Bytes.toBytes(id));

		return baos.toByteArray();
	}

}
