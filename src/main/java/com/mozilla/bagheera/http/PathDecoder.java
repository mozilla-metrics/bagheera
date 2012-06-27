/*
 * Copyright 2012 Mozilla Foundation
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
package com.mozilla.bagheera.http;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PathDecoder {

    private Pattern pathPattern = Pattern.compile("/([^/]+)");
    private List<String> pathElements = new ArrayList<String>();
    
    public PathDecoder(String uri) {
        Matcher m = pathPattern.matcher(uri);
        while (m.find()) {
            if (m.groupCount() > 0) {
                pathElements.add(m.group(1));
            }
        }
    }

    public String getPathElement(int idx) {
        return idx < pathElements.size() ? pathElements.get(idx) : null;
    }
    
}
