package com.mozilla.bagheera.http;

import static org.jboss.netty.handler.codec.http.HttpMethod.POST;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.UUID;

import org.junit.Test;

public class BagheeraHttpRequestTest {
    @Test
    public void testURIParsing() {
        String apiVersion = "1.0";
        String namespace = "bar";
        String endpoint = SubmissionHandler.ENDPOINT_SUBMIT;
        String id = "bogusid";
        String partitions = "part1/part2/part3";

        String uri = String.format("/%s/%s/%s/%s/%s", apiVersion, endpoint, namespace, id, partitions);
        BagheeraHttpRequest req = new BagheeraHttpRequest(HTTP_1_1, POST, uri);
        assertEquals(apiVersion, req.getApiVersion());
        assertEquals(namespace, req.getNamespace());
        assertEquals(endpoint, req.getEndpoint());
        assertEquals(id, req.getId());
        List<String> reqPartitions = req.getPartitions();
        assertEquals("part1", reqPartitions.get(0));
        assertEquals("part2", reqPartitions.get(1));
        assertEquals("part3", reqPartitions.get(2));

        req = new BagheeraHttpRequest(HTTP_1_1, POST, String.format("/%s/%s/%s", endpoint, namespace, id));
        assertEquals(null, req.getApiVersion());
        assertEquals(namespace, req.getNamespace());
        assertEquals(endpoint, req.getEndpoint());
        assertEquals(id, req.getId());
        assertEquals(0, req.getPartitions().size());

        req = new BagheeraHttpRequest(HTTP_1_1, POST, String.format("/%s/%s", endpoint, namespace));
        assertEquals(null, req.getApiVersion());
        assertEquals(namespace, req.getNamespace());
        assertEquals(endpoint, req.getEndpoint());
        // Should have an auto-assigned id:
        String randomId = req.getId();
        assertEquals(UUID.randomUUID().toString().length(), randomId.length());
        assertTrue(randomId.matches("^[0-9a-f-]{36}$"));
        UUID uuid = UUID.fromString(randomId);
        assertTrue(uuid != null);
        assertEquals(0, req.getPartitions().size());
    }

}
