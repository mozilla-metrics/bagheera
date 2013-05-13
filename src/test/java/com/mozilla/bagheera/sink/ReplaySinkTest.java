/*
 * Copyright 2013 Mozilla Foundation
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
package com.mozilla.bagheera.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

// Don't care that this httpserver stuff is restricted.
@SuppressWarnings("restriction")
public class ReplaySinkTest {
    int fakePort = 9888;
    String fakePath = "/submit/test";
    MyHandler requestHandler = new MyHandler();
    HttpServer server;

    @Before
    public void setup() throws IOException {
        // Set up a basic server:
        // See docs here: http://docs.oracle.com/javase/6/docs/jre/api/net/httpserver/spec/com/sun/net/httpserver/package-summary.html
        server = HttpServer.create(new InetSocketAddress(fakePort), 0);
        server.createContext(fakePath, requestHandler);
        server.setExecutor(null); // default executor
        server.start();
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void testReplayWithoutSampling() throws IOException {
        ReplaySink sink = new ReplaySink(getDestConfig("http://localhost:" + fakePort + fakePath + "/" + ReplaySink.KEY_PLACEHOLDER));

        sink.store("foo", "bar".getBytes());
        assertEquals(fakePath + "/foo", requestHandler.lastRequestURI.toString());

        // Make sure we see each request.
        int counter = 0;
        int max = 50;
        for (int i = 0; i < max; i++) {
            String key = "test" + i;
            byte[] payload = ("bar" + i).getBytes();
            sink.store(key, payload);
            String expectedURI = fakePath + "/" + key;
            if (expectedURI.equals(requestHandler.lastRequestURI.toString())) {
                counter++;
            }
        }

        // Without sampling, we should see all requests.
        assertEquals(max, counter);
    }

    @Test
    public void testReplayWithSampling() throws IOException {
        SinkConfiguration config = getDestConfig("http://localhost:" + fakePort + fakePath + "/" + ReplaySink.KEY_PLACEHOLDER);
        // Override sample rate:
        config.setString("replaysink.sample", "0.1");
        ReplaySink sink = new ReplaySink(config);

        // Make sure we see some, but not all, requests
        int counter = 0;
        int max = 500;
        for (int i = 0; i < max; i++) {
            String key = "test" + i;
            byte[] payload = ("bar" + i).getBytes();
            sink.store(key, payload);
            String expectedURI = fakePath + "/" + key;
            String actualURI = "";
            if (requestHandler.lastRequestURI != null) {
                actualURI = requestHandler.lastRequestURI.toString();
            }
            if (expectedURI.equals(actualURI)) {
                counter++;
            }
        }

        // With sampling, we should see some requests, but not all of them.
        assertTrue(counter > 0);
        assertTrue(counter < max);
    }

    @Test
    public void testDestNoSuffix() throws IOException {
        SinkConfiguration config = getDestConfig("http://localhost:8080/submit/foof/%k");
        ReplaySink sink = new ReplaySink(config);

        assertEquals("http://localhost:8080/submit/foof/test1", sink.getDest("test1"));
        assertEquals("http://localhost:8080/submit/foof/test2", sink.getDest("test2"));
        assertEquals("http://localhost:8080/submit/foof/a/b/c", sink.getDest("a/b/c"));
    }

    @Test
    public void testDestNoKey() throws IOException {
        SinkConfiguration config = getDestConfig("I am a test");
        ReplaySink sink = new ReplaySink(config);

        assertEquals("I am a test", sink.getDest("test1"));
        assertEquals("I am a test", sink.getDest("test2"));
        assertEquals("I am a test", sink.getDest("a/b/c"));
    }

    // FIXME We can't use the KeyValueSinkFactory because it gets stuck with the original config :(
    public SinkConfiguration getDestConfig(String destPattern) {
        SinkConfiguration config = new SinkConfiguration();
        config.setString("replaysink.keys", "true");
        config.setString("replaysink.delete", "true");
        config.setString("replaysink.sample", "1");
        config.setString("replaysink.dest", destPattern);

        return config;
    }

    @Test
    public void testDestSuffix() throws IOException {
        SinkConfiguration config = getDestConfig("foo %k bar");
        ReplaySink sink = new ReplaySink(config);

        assertEquals("foo test1 bar", sink.getDest("test1"));
        assertEquals("foo test2 bar", sink.getDest("test2"));
        assertEquals("foo a/b/c bar", sink.getDest("a/b/c"));
    }

    @Test
    public void testDeleteWithoutSampling() throws IOException {
        ReplaySink sink = new ReplaySink(getDestConfig("http://localhost:" + fakePort + fakePath + "/" + ReplaySink.KEY_PLACEHOLDER));

        // Make sure we see each request.
        int counter = 0;
        int max = 50;
        for (int i = 0; i < max; i++) {
            String key = "delete" + i;
            sink.delete(key);
            String expectedURI = fakePath + "/" + key;
            if (expectedURI.equals(requestHandler.lastRequestURI.toString())) {
                counter++;
            }
        }

        // Without sampling, we should see all delete requests.
        assertEquals(max, counter);
    }

    @Test
    public void testDisabledDeletes() throws IOException {
        SinkConfiguration config = getDestConfig("http://localhost:" + fakePort + fakePath + "/" + ReplaySink.KEY_PLACEHOLDER);
        config.setString("replaysink.delete", "false");
        ReplaySink sink = new ReplaySink(config);

        // Make sure we don't process any delete requests.
        int counter = 0;
        int max = 10;
        for (int i = 0; i < max; i++) {
            String key = "delete" + i;
            sink.delete(key);
            String expectedURI = fakePath + "/" + key;
            String actualURI = "";
            if (requestHandler.lastRequestURI != null) {
                actualURI = requestHandler.lastRequestURI.toString();
            }
            if (expectedURI.equals(actualURI)) {
                counter++;
            }
        }

        // Without sampling, we should see all delete requests.
        assertEquals(0, counter);
    }

    class MyHandler implements HttpHandler {
        public URI lastRequestURI;
        @Override
        public void handle(HttpExchange t) throws IOException {
            // TODO: this doesn't include the full URL, can we do better?
            lastRequestURI = t.getRequestURI();

            String response = "This is the response";
            t.sendResponseHeaders(200, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }
}
