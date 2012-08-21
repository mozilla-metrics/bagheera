package com.mozilla.bagheera.sink;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public class SequenceFileSink implements Sink, KeyValueSink {

    private static final Logger LOG = Logger.getLogger(SequenceFileSink.class);
    
    protected static final long DAY_IN_MILLIS = 86400000L;
    
    protected long sleepTime = 1000L;
    
    // HDFS related member vars
    protected Configuration conf;
    protected FileSystem hdfs;
    protected Path baseDir;
    protected SequenceFile.Writer writer;
    protected boolean useBytesValue;
    protected Text outputKey = new Text();
    protected Writable outputValue;
    protected long prevRolloverMillis = 0L;
    protected long bytesWritten = 0L;
    protected long maxFileSize = 0L;
    protected SimpleDateFormat sdf;

    public SequenceFileSink(String namespace, String baseDirPath, String dateFormat, long maxFileSize, boolean useBytesValue) throws IOException {
        this.useBytesValue = useBytesValue;
        if (useBytesValue) {
            outputValue = new BytesWritable();
        } else {
            outputKey = new Text();
        }
        
        LOG.info("Initializing writer for map: " + namespace);
        conf = new Configuration();
        conf.setBoolean("fs.automatic.close", false);
        hdfs = FileSystem.newInstance(conf);
        baseDir = new Path(baseDirPath);
        this.maxFileSize = maxFileSize;
        sdf = new SimpleDateFormat(dateFormat);
        Calendar cal = Calendar.getInstance();
        if (!baseDirPath.endsWith(Path.SEPARATOR)) {
            baseDir = new Path(baseDirPath + Path.SEPARATOR + namespace + Path.SEPARATOR + sdf.format(cal.getTime()));
        } else {
            baseDir = new Path(baseDirPath + namespace + Path.SEPARATOR + sdf.format(cal.getTime()));
        }
    }
    
    protected void initWriter() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Thread " + Thread.currentThread().getId() + " - initWriter() called");
        }
        
        if (!hdfs.exists(baseDir)) {
            hdfs.mkdirs(baseDir);
        }

        Path outputPath = new Path(baseDir, new Path(UUID.randomUUID().toString()));
        LOG.info("Opening file handle to: " + outputPath.toString());
        
        writer = SequenceFile.createWriter(hdfs, hdfs.getConf(), outputPath, outputKey.getClass(), outputValue.getClass(), CompressionType.BLOCK);
        
        // Get time in millis at a day resolution
        Calendar prev = Calendar.getInstance();
        prev.set(Calendar.HOUR_OF_DAY, 0);
        prev.set(Calendar.MINUTE, 0);
        prev.set(Calendar.SECOND, 0);
        prev.set(Calendar.MILLISECOND, 0);
        prevRolloverMillis = prev.getTimeInMillis();
    }
    
    private synchronized void checkRollover() throws IOException {
        boolean getNewFile = false;
        Calendar now = Calendar.getInstance();
        if (maxFileSize != 0 && bytesWritten >= maxFileSize) {
            getNewFile = true;
        } else if (now.getTimeInMillis() > (prevRolloverMillis + DAY_IN_MILLIS)) {
            getNewFile = true;
            baseDir = new Path(baseDir.getParent(), new Path(sdf.format(now.getTime())));
        }

        if (writer == null || getNewFile) {
            closeWriter();
            initWriter();
        }
    }
    
    protected synchronized void closeWriter() throws IOException {
        if (writer != null) {
            writer.close();
            writer = null;
        }
        bytesWritten = 0;
    }
    
    public void close() {
        try {
            closeWriter();
        } catch (IOException e) {
            LOG.error("Error closing writer", e);
        }

        if (hdfs != null) {
            try {
                hdfs.close();
            } catch (IOException e) {
                LOG.error("Error closing HDFS handle", e);
            }
        }
    }

    @Override
    public void store(byte[] data) {
        throw new RuntimeException("store is not implemented by SequeceFileSink yet");
    }

    @Override
    public void store(byte[] key, byte[] data) {
        try {
            outputKey.set(key);
            if (useBytesValue) {
                ((BytesWritable)outputValue).set(data, 0, data.length);
            } else {
                ((Text)outputValue).set(data);
            }
            
            checkRollover();
            writer.append(outputKey, outputValue);
            bytesWritten += key.length + data.length;
        } catch (IOException e) {
            LOG.error("IOException while writing key/value pair", e);
            throw new RuntimeException(e);
        }
    }
    
}
