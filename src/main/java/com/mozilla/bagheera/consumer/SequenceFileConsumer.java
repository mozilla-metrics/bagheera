/*
 * Copyright 2011 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.bagheera.consumer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;

public class SequenceFileConsumer {

    private static final Logger LOG = Logger.getLogger(SequenceFileConsumer.class);
    
    private static final long DAY_IN_MILLIS = 86400000L;
    
    private long sleepTime = 1000L;
    
    // HDFS related member vars
    private Configuration conf;
    private FileSystem hdfs;
    private Path baseDir;
    private SequenceFile.Writer writer;
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private long prevRolloverMillis = 0L;
    private long bytesWritten = 0L;
    private long maxFileSize = 0L;
    private SimpleDateFormat sdf;
    
    // Hazelcast related member vars
    private Map<String, String> nsMap;

    public SequenceFileConsumer(String mapName, String baseDirPath, String dateFormat, long maxFileSize,
                                String hzGroupName, String hzGroupPassword, String[] hzClients) throws IOException {
        LOG.info("Initializing writer for map: " + mapName);
        conf = new Configuration();
        conf.setBoolean("fs.automatic.close", false);
        hdfs = FileSystem.newInstance(conf);
        baseDir = new Path(baseDirPath);
        this.maxFileSize = maxFileSize;
        sdf = new SimpleDateFormat(dateFormat);
        Calendar cal = Calendar.getInstance();
        if (!baseDirPath.endsWith(Path.SEPARATOR)) {
            baseDir = new Path(baseDirPath + Path.SEPARATOR + mapName + Path.SEPARATOR + sdf.format(cal.getTime()));
        } else {
            baseDir = new Path(baseDirPath + mapName + Path.SEPARATOR + sdf.format(cal.getTime()));
        }        
        
        ClientConfig config = new ClientConfig();
        config.addAddress(hzClients);
        config.getGroupConfig().setName(hzGroupName).setPassword(hzGroupPassword);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        nsMap = client.getMap(mapName);
    }
    
    private void initWriter() throws IOException {
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
    
    private synchronized void closeWriter() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Thread " + Thread.currentThread().getId() + " - closeWriter() called");
        }
        
        if (writer != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Thread " + Thread.currentThread().getId() + " - writer.close() called");
            }
            writer.close();
            writer = null;
        }
        bytesWritten = 0;
    }
    
    public void close() {
        if (hdfs != null) {
            try {
                hdfs.close();
            } catch (IOException e) {
                LOG.error("Error closing HDFS handle", e);
            }
        }
    }
    
    public void poll() throws InterruptedException {
        while (true) {
            if (nsMap.size() > 0) {
                long startTime = System.currentTimeMillis(); 
                try {
                    checkRollover();
                    for (String k : nsMap.keySet()) {
                        String v = nsMap.remove(k);
                        if (v != null) {
                            outputKey.set(k);
                            bytesWritten += outputKey.getLength();
                            outputValue.set(v);
                            bytesWritten += outputValue.getLength();
                            writer.append(outputKey, outputValue);
                        }
                    }
                } catch (IOException e) {
                    LOG.error("IOException while writing key/value pair", e);
                    throw new RuntimeException(e);
                }
                
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Stored %d items in %d ms", nsMap.size(), (System.currentTimeMillis() - startTime)));
                }
            } else {
                Thread.sleep(sleepTime);
            }
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException {
        Options options = new Options();
        Option mapName = new Option("m", "map", true, "Name of map in Hazelcast.");
        mapName.setRequired(true);
        options.addOption(mapName);
        options.addOption(new Option("o", "outputdir", true, "Base output directory path."));
        options.addOption(new Option("df", "dateformat", true, "Output subdirectories date format."));
        options.addOption(new Option("fs", "filesize", true, "Maximum output file size."));
        options.addOption(new Option("gn", "groupname", true, "Hazelcast group name."));
        options.addOption(new Option("gp", "grouppassword", true, "Hazelcast group password."));
        options.addOption(new Option("hzservers", true, "Hazelcast server list."));
        
        CommandLineParser parser = new GnuParser();
        SequenceFileConsumer consumer = null;
        try {
            CommandLine cmd = parser.parse(options, args);
            consumer = new SequenceFileConsumer(mapName.getValue(), 
                                                cmd.getOptionValue("outputdir", "/bagheera"),
                                                cmd.getOptionValue("dateformat", "yyyy-MM-dd"),
                                                Long.parseLong(cmd.getOptionValue("filesize", "1073741824")),
                                                cmd.getOptionValue("groupname","bagheera"), cmd.getOptionValue("grouppassword","bagheera"),
                                                cmd.getOptionValue("hzservers","localhost:5701").split(","));
            consumer.poll();            
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("SequenceFileConsumer", options);
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }
    
}
