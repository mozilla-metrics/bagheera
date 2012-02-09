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
package com.mozilla.bagheera.rest.interceptors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

public class MetricsPingPreCommitTest {
    private MetricsPingPreCommit metricsPingPreCommit;
    private ObjectMapper objectMapper;

    @Before
    public void setup() {
        metricsPingPreCommit = new MetricsPingPreCommit();
        objectMapper = new ObjectMapper();
    }

    @Test
    public void testAggregate() {
        JsonNode validDoc;
        try {
            String validDocSrc = "{\"ver\":2,\"lastPingTime\":\"2012-02-03\",\"thisPingTime\":\"2012-02-08\",\"env\":{\"reason\":\"startup\",\"OS\":\"Linux\",\"appID\":\"{ec8030f7-c20a-464f-9b0e-13a3a9e97384}\",\"appVersion\":\"12.0a1\",\"appVendor\":\"Mozilla\",\"appName\":\"Firefox\",\"appBuildID\":\"20120208143624\",\"appABI\":\"x86_64-gcc3\",\"appUpdateChannel\":\"default\",\"appDistribution\":\"default\",\"appDistributionVersion\":\"default\",\"appHotfixVersion\":\"\",\"platformBuildID\":\"20120126141109\",\"platformVersion\":\"12.0a1\",\"locale\":\"en-US\",\"name\":\"Linux\",\"version\":\"3.0.0-15-generic\",\"cpucount\":4,\"memsize\":7889,\"arch\":\"x86-64\"},\"addons\":[{\"id\":\"crashme@ted.mielczarek.org\",\"userDisabled\":false,\"appDisabled\":false,\"version\":\"0.3\",\"type\":\"extension\",\"hasBinaryComponents\":false,\"installDate\":\"2011-10-25\",\"updateDate\":\"2011-10-25\"},{\"id\":\"ping.telemetry@mozilla.com\",\"userDisabled\":false,\"appDisabled\":false,\"version\":\"0.5\",\"type\":\"extension\",\"hasBinaryComponents\":false,\"installDate\":\"2011-11-16\",\"updateDate\":\"2011-12-20\"},{\"id\":\"about.blank@mozilla.com\",\"userDisabled\":false,\"appDisabled\":false,\"version\":\"0.5\",\"type\":\"extension\",\"hasBinaryComponents\":false,\"installDate\":\"2012-01-27\",\"updateDate\":\"2012-02-01\"},{\"id\":\"{e2c52c1c-5ee1-cc23-15fa-35945fd58806}\",\"userDisabled\":false,\"appDisabled\":false,\"version\":\"1.0.0.0\",\"type\":\"plugin\",\"installDate\":\"2012-01-26\",\"updateDate\":\"2012-01-26\"},{\"id\":\"{18965679-bddd-de62-52b4-b56e6316d854}\",\"userDisabled\":false,\"appDisabled\":false,\"version\":\"\",\"type\":\"plugin\",\"installDate\":\"2011-12-13\",\"updateDate\":\"2011-12-13\"},{\"id\":\"{5c0830c7-3003-fc43-0daf-d29b579f5f6b}\",\"userDisabled\":false,\"appDisabled\":false,\"version\":\"\",\"type\":\"plugin\",\"installDate\":\"2011-12-13\",\"updateDate\":\"2011-12-13\"},{\"id\":\"{ed5c33eb-95c1-f1be-50ba-eb0ade42d912}\",\"userDisabled\":false,\"appDisabled\":false,\"version\":\"\",\"type\":\"plugin\",\"installDate\":\"2011-12-02\",\"updateDate\":\"2011-12-02\"},{\"id\":\"{79eb71d7-19b2-ef97-2247-9a8960804972}\",\"userDisabled\":false,\"appDisabled\":false,\"version\":\"\",\"type\":\"plugin\",\"installDate\":\"2011-11-08\",\"updateDate\":\"2011-11-08\"},{\"id\":\"{f2d261dc-c5c4-ca3c-ae02-ccb3ff227c7f}\",\"userDisabled\":false,\"appDisabled\":false,\"version\":\"\",\"type\":\"plugin\",\"installDate\":\"2011-10-18\",\"updateDate\":\"2011-10-18\"},{\"id\":\"{84e372b2-f1a3-032a-0001-b725ee38d1ed}\",\"userDisabled\":false,\"appDisabled\":false,\"version\":\"\",\"type\":\"plugin\",\"installDate\":\"2011-10-18\",\"updateDate\":\"2011-10-18\"},{\"id\":\"{fcdc99b5-45d4-daeb-9239-0a41e6c9b7ce}\",\"userDisabled\":false,\"appDisabled\":false,\"version\":\"\",\"type\":\"plugin\",\"installDate\":\"2011-10-18\",\"updateDate\":\"2011-10-18\"},{\"id\":\"{24f3e033-1a7c-ae8b-3fc7-4ac494c18e91}\",\"userDisabled\":false,\"appDisabled\":false,\"version\":\"\",\"type\":\"plugin\",\"installDate\":\"2011-10-18\",\"updateDate\":\"2011-10-18\"}],\"currentSessionTime\":60,\"currentSessionActiveTime\":70,\"versions\":[[\"2012-01-30\",\"12.0a1\"]],\"dataPoints\":{\"2012-02-08\":{\"sessions\":{\"completedSessions\":3,\"completedSessionTime\":766,\"completedSessionActiveTime\":265},\"simpleMeasurements\":{\"uptime\":2,\"main\":22,\"firstPaint\":689,\"sessionRestored\":561,\"isDefaultBrowser\":false,\"crashCountSubmitted\":0,\"profileAge\":127,\"placesPagesCount\":513,\"placesBookmarksCount\":77,\"addonCount\":14}},\"2012-02-06\":{\"simpleMeasurements\":{\"uptime\":2,\"main\":174,\"firstPaint\":994,\"sessionRestored\":868,\"isDefaultBrowser\":false,\"crashCountSubmitted\":0,\"profileAge\":124,\"placesPagesCount\":509,\"placesBookmarksCount\":73,\"addonCount\":14},\"sessions\":{\"completedSessions\":1,\"completedSessionTime\":339,\"completedSessionActiveTime\":35}},\"2012-02-05\":{\"sessions\":{\"completedSessions\":2,\"completedSessionTime\":3062,\"completedSessionActiveTime\":445},\"simpleMeasurements\":{\"uptime\":2,\"main\":190,\"firstPaint\":1300,\"sessionRestored\":1160,\"isDefaultBrowser\":false,\"crashCountSubmitted\":0,\"profileAge\":123,\"placesPagesCount\":525,\"placesBookmarksCount\":73,\"addonCount\":14}},\"2012-02-04\":{\"simpleMeasurements\":{\"uptime\":2,\"main\":10,\"firstPaint\":587,\"sessionRestored\":470,\"isDefaultBrowser\":false,\"crashCountSubmitted\":0,\"profileAge\":123,\"placesPagesCount\":530,\"placesBookmarksCount\":72,\"addonCount\":14},\"sessions\":{\"completedSessions\":5,\"completedSessionTime\":8917,\"completedSessionActiveTime\":1420}},\"2012-02-03\":{\"simpleMeasurements\":{\"uptime\":1,\"main\":13,\"firstPaint\":553,\"sessionRestored\":454,\"isDefaultBrowser\":false,\"crashCountSubmitted\":0},\"sessions\":{\"completedSessions\":17,\"completedSessionTime\":22065,\"completedSessionActiveTime\":3550}},\"2012-02-02\":{\"search\":{\"searchbar\":{\"Google\":1},\"abouthome\":{\"Google\":1}},\"sessions\":{\"completedSessions\":6,\"completedSessionTime\":4879,\"completedSessionActiveTime\":815},\"simpleMeasurements\":{\"uptime\":2,\"main\":13,\"firstPaint\":537,\"sessionRestored\":398,\"isDefaultBrowser\":false,\"crashCountSubmitted\":0,\"profileAge\":121,\"placesPagesCount\":463,\"placesBookmarksCount\":77,\"addonCount\":14,\"version\":\"12.0a1\"}},\"2012-02-01\":{\"simpleMeasurements\":{\"uptime\":2,\"main\":17,\"firstPaint\":582,\"sessionRestored\":468,\"isDefaultBrowser\":false,\"crashCountSubmitted\":0,\"profileAge\":120,\"placesPagesCount\":456,\"placesBookmarksCount\":76,\"addonCount\":14},\"sessions\":{\"completedSessions\":10,\"completedSessionTime\":5852,\"completedSessionActiveTime\":780,\"abortedSessions\":1,\"abortedSessionTime\":222,\"abortedSessionActiveTime\":55}},\"2012-01-31\":{\"search\":{\"abouthome\":{\"Google\":1}},\"sessions\":{\"completedSessions\":3,\"completedSessionTime\":73096,\"completedSessionActiveTime\":310},\"simpleMeasurements\":{\"uptime\":2,\"main\":10,\"firstPaint\":566,\"sessionRestored\":446,\"isDefaultBrowser\":false,\"crashCountSubmitted\":0,\"profileAge\":119,\"placesPagesCount\":452,\"placesBookmarksCount\":77,\"addonCount\":14}},\"2012-01-30\":{\"sessions\":{\"completedSessions\":10,\"completedSessionTime\":2202,\"completedSessionActiveTime\":640,\"abortedSessions\":2,\"abortedSessionTime\":60,\"abortedSessionActiveTime\":50},\"search\":{\"abouthome\":{\"Google\":1},\"searchbar\":{\"Bing\":1,\"Other\":1,\"Google\":2}},\"simpleMeasurements\":{\"uptime\":2,\"main\":11,\"firstPaint\":698,\"sessionRestored\":564,\"isDefaultBrowser\":false,\"crashCountSubmitted\":0,\"profileAge\":118,\"placesPagesCount\":479,\"placesBookmarksCount\":74,\"addonCount\":14}}}}";
            validDoc = objectMapper.readValue(validDocSrc, JsonNode.class);
            assertTrue(metricsPingPreCommit.isValidDocument(validDoc));

            String invalidDocSrc = "{}";
            JsonNode invalidDoc = objectMapper.readValue(invalidDocSrc, JsonNode.class);
            assertFalse(metricsPingPreCommit.isValidDocument(invalidDoc));

            // Missing "dataPoints"
            invalidDocSrc = "{\"ver\":2,\"lastPingTime\":\"2012-02-03\",\"thisPingTime\":\"2012-02-08\",\"env\":{\"reason\":\"startup\",\"OS\":\"Linux\",\"appID\":\"{ec8030f7-c20a-464f-9b0e-13a3a9e97384}\",\"appVersion\":\"12.0a1\",\"appVendor\":\"Mozilla\",\"appName\":\"Firefox\",\"appBuildID\":\"20120208143624\",\"appABI\":\"x86_64-gcc3\",\"appUpdateChannel\":\"default\",\"appDistribution\":\"default\",\"appDistributionVersion\":\"default\",\"appHotfixVersion\":\"\",\"platformBuildID\":\"20120126141109\",\"platformVersion\":\"12.0a1\",\"locale\":\"en-US\",\"name\":\"Linux\",\"version\":\"3.0.0-15-generic\",\"cpucount\":4,\"memsize\":7889,\"arch\":\"x86-64\"},\"addons\":[{\"id\":\"crashme@ted.mielczarek.org\",\"userDisabled\":false,\"appDisabled\":false,\"version\":\"0.3\",\"type\":\"extension\",\"hasBinaryComponents\":false,\"installDate\":\"2011-10-25\",\"updateDate\":\"2011-10-25\"},{\"id\":\"{24f3e033-1a7c-ae8b-3fc7-4ac494c18e91}\",\"userDisabled\":false,\"appDisabled\":false,\"version\":\"\",\"type\":\"plugin\",\"installDate\":\"2011-10-18\",\"updateDate\":\"2011-10-18\"}],\"currentSessionTime\":60,\"currentSessionActiveTime\":70,\"versions\":[[\"2012-01-30\",\"12.0a1\"]]}";
            invalidDoc = objectMapper.readValue(invalidDocSrc, JsonNode.class);
            assertFalse(metricsPingPreCommit.isValidDocument(invalidDoc));

            // Empty "dataPoints"
            validDocSrc = "{\"ver\":2,\"lastPingTime\":\"2012-02-03\",\"thisPingTime\":\"2012-02-08\",\"env\":{\"reason\":\"startup\",\"OS\":\"Linux\",\"appID\":\"{ec8030f7-c20a-464f-9b0e-13a3a9e97384}\",\"appVersion\":\"12.0a1\",\"appVendor\":\"Mozilla\",\"appName\":\"Firefox\",\"appBuildID\":\"20120208143624\",\"appABI\":\"x86_64-gcc3\",\"appUpdateChannel\":\"default\",\"appDistribution\":\"default\",\"appDistributionVersion\":\"default\",\"appHotfixVersion\":\"\",\"platformBuildID\":\"20120126141109\",\"platformVersion\":\"12.0a1\",\"locale\":\"en-US\",\"name\":\"Linux\",\"version\":\"3.0.0-15-generic\",\"cpucount\":4,\"memsize\":7889,\"arch\":\"x86-64\"},\"addons\":[{\"id\":\"crashme@ted.mielczarek.org\",\"userDisabled\":false,\"appDisabled\":false,\"version\":\"0.3\",\"type\":\"extension\",\"hasBinaryComponents\":false,\"installDate\":\"2011-10-25\",\"updateDate\":\"2011-10-25\"},{\"id\":\"{24f3e033-1a7c-ae8b-3fc7-4ac494c18e91}\",\"userDisabled\":false,\"appDisabled\":false,\"version\":\"\",\"type\":\"plugin\",\"installDate\":\"2011-10-18\",\"updateDate\":\"2011-10-18\"}],\"currentSessionTime\":60,\"currentSessionActiveTime\":70,\"versions\":[[\"2012-01-30\",\"12.0a1\"]],\"dataPoints\":{}}";
            validDoc = objectMapper.readValue(validDocSrc, JsonNode.class);
            assertTrue(metricsPingPreCommit.isValidDocument(validDoc));

            // minimal
            validDocSrc = "{\"lastPingTime\":\"2012-02-03\",\"thisPingTime\":\"2012-02-08\",\"env\":{},\"addons\":[],\"versions\":[],\"dataPoints\":{}}";
            validDoc = objectMapper.readValue(validDocSrc, JsonNode.class);
            assertTrue(metricsPingPreCommit.isValidDocument(validDoc));

            // beyond minimal
            invalidDocSrc = "{\"lastPingTime\":\"2012-02-03\",\"env\":{},\"addons\":[],\"versions\":[],\"dataPoints\":{}}";
            invalidDoc = objectMapper.readValue(invalidDocSrc, JsonNode.class);
            assertFalse(metricsPingPreCommit.isValidDocument(invalidDoc));
        } catch (JsonParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (JsonMappingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
