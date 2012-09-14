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
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

/**
 * Utility class for id generation and bucketing
 */
public class IdUtil {

	public static final int DEFAULT_NUM_BUCKETS = Byte.MAX_VALUE;
	public static final int HEX_BUCKETS = 16;

	public static final SimpleDateFormat SDF = new SimpleDateFormat("yyyyMMdd");

	/**
	 * @return
	 */
	public static byte[] generateNonBucketizedId() {
	    return UUID.randomUUID().toString().getBytes();
	}
	
	/**
	 * Generates a bucket/date prefixed random id
	 * @return
	 * @throws IOException
	 */
	public static byte[] generateBucketizedId() throws IOException {
		return nonRandByteBucketizeId(UUID.randomUUID().toString(), Calendar.getInstance().getTime());
	}

	/**
	 * Adds a bucket/date prefix to an given string id
	 * @param id
	 * @return
	 * @throws IOException
	 */
	public static byte[] bucketizeId(String id) throws IOException {
	    if (id == null) {
            throw new IllegalArgumentException("id cannot be null");
        }
	    
		return nonRandByteBucketizeId(id, Calendar.getInstance().getTime());
	}

	/**
	 * @param id
	 * @param timestamp
	 * @return
	 * @throws IOException
	 */
	public static byte[] bucketizeId(String id, long timestamp) throws IOException {
        if (id == null) {
            throw new IllegalArgumentException("id cannot be null");
        }
        
        return nonRandByteBucketizeId(id, new Date(timestamp));
    }
	
	/**
	 * Takes a given id and prefixes it with a byte character and the date in a non-random fashion
	 * This method expects id to be something like a UUID consisting only of hex characters.  If id 
	 * is something else this will produce unpredictable results.
	 * @param id
	 * @param d
	 * @return
	 * @throws IOException
	 */
	public static byte[] nonRandByteBucketizeId(String id, Date d) throws IOException {
		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("id cannot be null or empty");
		}
        if (d == null) {
            throw new IllegalArgumentException("date cannot be null");
        }
        
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		int bucket = 0;
		if (id.length() >= 2) {
		    // Munge two hex characters into the range of a single byte
		    bucket = Integer.parseInt(id.substring(0, 2), 16) - 128;
		} else {
		    bucket = Integer.parseInt(id, 16) - 128;
		}
		baos.write(bucket);
		baos.write(SDF.format(d).getBytes());
		baos.write(id.getBytes());

		return baos.toByteArray();
	}
	
	/**
	 * Takes a given id and prefixes it with a hex character and the date in a non-random fashion
	 * @param id
	 * @param d
	 * @return
	 * @throws IOException
	 */
	public static byte[] nonRandHexBucketizeId(String id, Date d) throws IOException {
	    if (id == null) {
            throw new IllegalArgumentException("id cannot be null");
        }
        if (d == null) {
            throw new IllegalArgumentException("date cannot be null");
        }
        
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		baos.write(id.charAt(0));
		baos.write(SDF.format(d).getBytes());
		baos.write(id.getBytes());

		return baos.toByteArray();
	}

}
