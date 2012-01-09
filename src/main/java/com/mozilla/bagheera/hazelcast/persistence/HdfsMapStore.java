package com.mozilla.bagheera.hazelcast.persistence;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;

public abstract class HdfsMapStore extends MapStoreBase implements MapStore<String, String>, MapLoaderLifecycleSupport {

    private static final Logger LOG = Logger.getLogger(HdfsMapStore.class);
    
    protected static final long DAY_IN_MILLIS = 86400000L;
    
    // Only keep one HDFS handle and configuration for all instances
    private static Configuration conf = new Configuration();
    protected static FileSystem hdfs;
    // Using an atomic int rather than countdown latch because we need to count up too
    protected static final AtomicInteger instanceCount = new AtomicInteger(0);
    
    protected Path baseDir;
    protected long bytesWritten = 0;
    protected SimpleDateFormat sdf;
    protected long maxFileSize = 0;
    protected long prevRolloverMillis = 0;
        
    /**
     * Synchronized method to make sure we initiliaze only once for this application
     */
    protected synchronized void initHDFS() {
        instanceCount.incrementAndGet();
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Thread " + Thread.currentThread().getId() + " - initHDFS called");
        }
    
        if (hdfs == null) {
            try {
                hdfs = FileSystem.get(conf);
            } catch (IOException e) {
                LOG.error("Error getting HDFS handle", e);
                throw new RuntimeException(e);
            }
        }
    }
    
    /**
     * 
     */
    protected synchronized void closeHDFS() {
        instanceCount.decrementAndGet();
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Thread " + Thread.currentThread().getId() + " - closeHDFS called");
            LOG.debug("Instance count == " + instanceCount.get());
        }
        
        if (instanceCount.get() <= 0 && hdfs != null) {
            try {
                hdfs.close();
            } catch (IOException e) {
                LOG.error("Error closing HDFS handle", e);
            }
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.hazelcast.core.MapLoaderLifecycleSupport#init(com.hazelcast.core.
     * HazelcastInstance, java.util.Properties, java.lang.String)
     */
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        super.init(hazelcastInstance, properties, mapName);

        for (String name : properties.stringPropertyNames()) {
            if (name.startsWith("hadoop.")) {
                conf.set(name, properties.getProperty(name));
            }
        }
        // set fs.automatic.close=false because this will cause writer.close() issues if true
        conf.setBoolean("fs.automatic.close", false);
        // initialize HDFS
        initHDFS();
        
        String hdfsBaseDir = properties.getProperty("hazelcast.hdfs.basedir", "/bagheera");
        String dateFormat = properties.getProperty("hazelcast.hdfs.dateformat", "yyyy-MM-dd");
        sdf = new SimpleDateFormat(dateFormat);
        Calendar cal = Calendar.getInstance();
        if (!hdfsBaseDir.endsWith(Path.SEPARATOR)) {
            baseDir = new Path(hdfsBaseDir + Path.SEPARATOR + mapName + Path.SEPARATOR + sdf.format(cal.getTime()));
        } else {
            baseDir = new Path(hdfsBaseDir + mapName + Path.SEPARATOR + sdf.format(cal.getTime()));
        }

        maxFileSize = Integer.parseInt(properties.getProperty("hazelcast.hdfs.max.filesize","0"));
        LOG.info("Using HDFS max file size: " + maxFileSize);
    }
    
    @Override
    public String load(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, String> loadAll(Collection<String> keys) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<String> loadAllKeys() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void destroy() {
        // TODO Auto-generated method stub
    }

    @Override
    public void store(String key, String value) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void storeAll(Map<String, String> map) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void delete(String key) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        // TODO Auto-generated method stub
        
    }

}
