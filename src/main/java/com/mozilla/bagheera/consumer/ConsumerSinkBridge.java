package com.mozilla.bagheera.consumer;

import com.mozilla.bagheera.sink.KeyValueSink;

public class ConsumerSinkBridge {

    private Consumer consumer;
    private KeyValueSink sink;
    
    public ConsumerSinkBridge(Consumer consumer, KeyValueSink sink) {
        this.consumer = consumer;
        this.sink = sink;
    }
    
    public void pipe() {
        
    }
    
    public static void main(String[] args) {
        
    }
}
