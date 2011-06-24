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
package com.mozilla.bagheera.model;

import java.io.Serializable;

/**
 * RequestData is intended to capture key pieces of information for a given
 * service request into a single object.
 */
public class RequestData implements Serializable {

	private static final long serialVersionUID = -4696306827814231622L;
	
	private final String userAgent;
	private final String ipAddress;
	private final byte[] payload;
	
	public RequestData(String userAgent, String ipAddress, byte[] payload) {
		this.userAgent = userAgent;
		this.ipAddress = ipAddress;
		this.payload = payload;
	}

	/**
	 * @return
	 */
	public String getUserAgent() {
		return userAgent;
	}

	/**
	 * @return
	 */
	public String getIpAddress() {
		return ipAddress;
	}

	/**
	 * @return
	 */
	public byte[] getPayload() {
		return payload;
	}
	
	/**
	 * @return
	 */
	public int getPayloadSize() {
	    return payload.length;
	}
	
}
