package com.mozilla.bagheera.producer;

import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;

public interface Producer {

    public void send(String namespace, String id, String data);
    public void send(BagheeraMessage msg);
    public void sendBytes(String namespace, String id, byte[] data);
    public void close();
    
}
