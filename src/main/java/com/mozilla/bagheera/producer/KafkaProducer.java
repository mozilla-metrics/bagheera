package com.mozilla.bagheera.producer;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

public class KafkaProducer implements com.mozilla.bagheera.producer.Producer {

    private static final String DELIMITER = "\u0001";
    private Producer<String, String> producer;
    
    public KafkaProducer(Properties props) {
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
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
    
}
