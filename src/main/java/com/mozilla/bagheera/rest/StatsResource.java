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
package com.mozilla.bagheera.rest;

import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.mozilla.bagheera.rest.stats.Stats;

@Path("/stats")
public class StatsResource extends ResourceBase {

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response getStats() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Stats> entry : rs.getStatsMap().entrySet()) {
            Stats stats = entry.getValue();
            sb.append("name=").append(entry.getKey()).append("\n");
            sb.append("numRequests=").append(stats.numRequests.get()).append("\n");
            sb.append("numValidRequests=").append(stats.numValidRequests.get()).append("\n");
            sb.append("numInvalidRequests=").append(stats.numInvalidRequests.get()).append("\n");
            sb.append("numForbiddenRequests=").append(stats.numForbiddenRequests.get()).append("\n");
            sb.append("numPuts=").append(stats.numPuts.get()).append("\n");
            sb.append("numGets=").append(stats.numGets.get()).append("\n");
            sb.append("numDels=").append(stats.numDels.get()).append("\n");
        }
        
        return Response.ok(sb.toString(), MediaType.TEXT_PLAIN).header("connection", "close").build();
    }

    @GET
    @Path("reset")
    public Response resetStats() {
        for (Map.Entry<String, Stats> entry : rs.getStatsMap().entrySet()) {
            entry.getValue().resetAll();
        }
        return Response.ok().header("connection", "close").build();
    }
}