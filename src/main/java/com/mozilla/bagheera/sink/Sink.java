package com.mozilla.bagheera.sink;

import java.io.IOException;

public interface Sink {

    public void store(byte[] data) throws IOException;

}
