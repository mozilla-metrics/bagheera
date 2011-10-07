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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/stats")
public class StatsResource extends ResourceBase {

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response getStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("numRequests=").append(stats.numRequests.get()).append("\n");
        sb.append("numPuts=").append(stats.numPuts.get()).append("\n");
        sb.append("numGets=").append(stats.numGets.get());
        return Response.ok(sb.toString(), MediaType.APPLICATION_JSON).build();
    }
    
    @GET
    @Path("reset")
    public void resetStats() {
        stats.resetAll();
    }
    
}