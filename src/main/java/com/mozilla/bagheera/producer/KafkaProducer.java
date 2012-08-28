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
package com.mozilla.bagheera.producer;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;

public class KafkaProducer implements com.mozilla.bagheera.producer.Producer {

    private static final String DELIMITER = "\u0001";
    private Producer<String,String> producer;
    private Producer<String,BagheeraMessage> messageProducer;
    
    public KafkaProducer(Properties props) {
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String,String>(config);
        messageProducer = new Producer<String,BagheeraMessage>(config);
    }
    
    /* (non-Javadoc)
     * @see com.mozilla.bagheera.producer.Producer#close()
     */
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
    
    /* (non-Javadoc)
     * @see com.mozilla.bagheera.producer.Producer#send(java.lang.String, java.lang.String, java.lang.String)
     */
    public void send(String namespace, String id, String data) {
        StringBuilder sb = new StringBuilder(id);
        sb.append(DELIMITER);
        sb.append(data);
        producer.send(new ProducerData<String,String>(namespace, sb.toString()));
    }
    
    /* (non-Javadoc)
     * @see com.mozilla.bagheera.producer.Producer#send(com.mozilla.bagheera.BagheeraProto.BagheeraMessage)
     */
    @Override
    public void send(BagheeraMessage msg) {
        messageProducer.send(new ProducerData<String,BagheeraMessage>(msg.getNamespace(), msg));
    }
    
}
