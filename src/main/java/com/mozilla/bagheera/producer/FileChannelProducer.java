package com.mozilla.bagheera.producer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.ReadableByteChannel;

public class FileChannelProducer implements Producer {

    private MappedByteBuffer buffer;
    private FileChannel channel;
    
    private final int bufferSize;
    private final int syncInterval;
    private int lastSyncPosition = 0;
    
    public FileChannelProducer(int bufferSize, int syncInterval) throws IOException {
        this.bufferSize = bufferSize;
        this.syncInterval = syncInterval;
        init();
    }
    
    @Override
    public void send(String namespace, String id, String data) {
        // TODO Auto-generated method stub       
        try {
            ByteBuffer buf = ByteBuffer.allocate(id.length() + data.length() + 1);
            buf.put(id.getBytes());
            buf.put("\u0001".getBytes());
            buf.put(data.getBytes());
            send(buf);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void send(ReadableByteChannel src, int numBytes) throws IOException {
        buffer.putInt(numBytes);
        channel.transferFrom(src, buffer.position(), numBytes);
    }
    
    public void send(ByteBuffer src) throws IOException {
        channel.write(src);
    }
    
    public void send(byte[] data) {
        if (buffer.remaining() < data.length) {
            // TODO: roll file
        }
        
        buffer.putInt(data.length);
        buffer.put(data);
        
        if (buffer.position() > (lastSyncPosition+syncInterval)) {
            buffer.force();
        }
    }
    
    private void init() throws IOException {
        if (buffer != null) {
            close();
        }

        String outputPath = "/tmp/foo";
        channel = new FileOutputStream(outputPath).getChannel();
        buffer = channel.map(MapMode.READ_WRITE, 0, bufferSize);
    }
    
    @Override
    public void close() {
        // TODO Auto-generated method stub
        try {
            channel.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
}
