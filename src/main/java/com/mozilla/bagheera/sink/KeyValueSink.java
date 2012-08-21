package com.mozilla.bagheera.sink;

import java.io.IOException;

public interface KeyValueSink {

    public void store(byte[] key, byte[] data) throws IOException;
    public void close();
    
}
