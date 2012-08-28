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
package com.mozilla.bagheera.serializer;

import kafka.message.Message;
import kafka.serializer.Decoder;

import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mozilla.bagheera.BagheeraProto;
import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;

public class BagheeraDecoder implements Decoder<BagheeraMessage> {

    private static final Logger LOG = Logger.getLogger(BagheeraDecoder.class);
    
    @Override
    public BagheeraMessage toEvent(Message msg) {
        BagheeraMessage bmsg = null;
        try {
            bmsg = BagheeraProto.BagheeraMessage.parseFrom(ByteString.copyFrom(msg.payload()));
        } catch (InvalidProtocolBufferException e) {
            LOG.error("Received unparseable message", e);
        }
        
        return bmsg;
    }

}
