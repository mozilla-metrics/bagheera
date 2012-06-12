package com.mozilla.bagheera.producer;

public interface Producer {

    public void send(String namespace, String id, String data);
    public void close();
    
}
