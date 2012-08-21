package com.mozilla.bagheera.sink;

import java.io.IOException;

import org.apache.log4j.Logger;

public class LoggerSink implements Sink, KeyValueSink {

    private static final Logger LOG = Logger.getLogger(LoggerSink.class);
    
    private boolean logValues;
    
    public LoggerSink(boolean logValues) {
        this.logValues = logValues;
    }
    
    @Override
    public void close() {
    }
    
    @Override
    public void store(byte[] data) throws IOException {
        LOG.info("data length:" + data.length + "\n");
        if (logValues) {
            LOG.info("data: " + new String(data, "UTF-8") + "\n");
        }
    }
    
    @Override
    public void store(byte[] key, byte[] data) throws IOException {
        // TODO Auto-generated method stub
        LOG.info("key length: " + key.length + "\n");
        LOG.info("data length:" + data.length + "\n");
        if (logValues) {
            LOG.info("key: " + new String(key, "UTF-8") + "\n");
            LOG.info("data: " + new String(data, "UTF-8" + "\n"));
        }
    }

}
