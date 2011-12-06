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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import com.hazelcast.core.Hazelcast;

public class MetricsPingPreCommitTest {
    private MetricsPingPreCommit metricsPingPreCommit;
	private ObjectMapper objectMapper;

	@Before
    public void setup() {
    	metricsPingPreCommit = new MetricsPingPreCommit();
    	objectMapper = new ObjectMapper();
    }
	
	@Test
	public void testEmptyAggregate() throws JsonGenerationException, JsonMappingException, IOException {
		org.codehaus.jackson.JsonNode node = metricsPingPreCommit.createEmptyAggregate();
		String expectedEmptyDoc = "{\"pingTime\":[],\"clockSkew\":[],\"env\":{},\"versions\":[],\"simpleMeasurements\":{},\"events\":{\"search\":{\"searchbar\":{\"Amazon.com\":[],\"Bing\":[],\"Google\":[],\"Yahoo\":[],\"Other\":[]},\"urlbar\":{\"Amazon.com\":[],\"Bing\":[],\"Google\":[],\"Yahoo\":[],\"Other\":[]},\"abouthome\":{\"Amazon.com\":[],\"Bing\":[],\"Google\":[],\"Yahoo\":[],\"Other\":[]},\"contextmenu\":{\"Amazon.com\":[],\"Bing\":[],\"Google\":[],\"Yahoo\":[],\"Other\":[]},\"total\":{\"Amazon.com\":0,\"Bing\":0,\"Google\":0,\"Yahoo\":0,\"Other\":0}},\"sessions\":{\"completedSessions\":[],\"completedSessionTime\":[],\"completedSessionActivityRatio\":[],\"abortedSessions\":[],\"abortedSessionTime\":[],\"abortedSessionActivityRatio\":[],\"abortedSessionMed\":[],\"currentSessionTime\":[],\"currentSessionActivityRatio\":[],\"aboutSessionRestoreStarts\":[]},\"corruptedEvents\":[]}}";
		String actualEmptyDoc = objectMapper.writeValueAsString(node);
		assertEquals(expectedEmptyDoc, actualEmptyDoc);
		
		assertTrue(metricsPingPreCommit.isValidAggregate(node));
	}

    
    public void testScriptEngine() {
    	ScriptEngineManager mgr = new ScriptEngineManager();
    	  List<ScriptEngineFactory> factories = 
    	      mgr.getEngineFactories();
    	  for (ScriptEngineFactory factory: factories) {
    	    System.out.println("ScriptEngineFactory Info");
    	    String engName = factory.getEngineName();
    	    String engVersion = factory.getEngineVersion();
    	    String langName = factory.getLanguageName();
    	    String langVersion = factory.getLanguageVersion();
    	    System.out.printf("\tScript Engine: %s (%s)\n", 
    	        engName, engVersion);
    	    List<String> engNames = factory.getNames();
    	    for(String name: engNames) {
    	      System.out.printf("\tEngine Alias: %s\n", name);
    	    }
    	    System.out.printf("\tLanguage: %s (%s)\n", 
    	        langName, langVersion);
    	  }
    }
    
    @Test
    public void testAggregate() {
    	String mapName = "metrics";
    	String id = "sample_id";
    	String newDocument1 = "{\"ver\":1,\"uuid\":\"e8a583fe-98ec-45be-9e44-96a23759067a\",\"lastPingTime\":\"2011-11-13T21:21:40.000Z\",\"thisPingTime\":\"2011-11-17T20:09:59.515Z\",\"currentTime\":\"2011-11-17T20:11:15.527Z\",\"env\":{\"reason\":\"idle-daily\",\"OS\":\"Linux\",\"appID\":\"{ec8030f7-c20a-464f-9b0e-13a3a9e97384}\",\"appVersion\":\"11.0a1\",\"appVendor\":\"Mozilla\",\"appName\":\"Firefox\",\"appBuildID\":\"20111117160821\",\"appABI\":\"x86_64-gcc3\",\"appUpdateChannel\":\"default\",\"appDistribution\":\"default\",\"appDistributionVersion\":\"default\",\"platformBuildID\":\"20111116163634\",\"platformVersion\":\"11.0a1\",\"locale\":\"en-US\",\"name\":\"Linux\",\"version\":\"2.6.38-12-generic\",\"cpucount\":4,\"memsize\":7889,\"arch\":\"x86-64\"},\"simpleMeasurements\":{\"uptime\":2,\"main\":19,\"firstPaint\":708,\"sessionRestored\":555,\"isDefaultBrowser\":false,\"crashCountSubmitted\":0,\"profileAge\":44,\"placesPagesCount\":1202,\"placesBookmarksCount\":75,\"addonCount\":14,\"addons\":[{\"id\":\"crashme@ted.mielczarek.org\",\"appDisabled\":false,\"version\":\"0.3\",\"installDate\":\"2011-10-25T15:02:03.000Z\",\"updateDate\":\"2011-10-25T15:02:03.000Z\"},{\"id\":\"ubufox@ubuntu.com\",\"appDisabled\":true,\"version\":\"0.9.2\",\"installDate\":\"2011-09-30T11:19:22.000Z\",\"updateDate\":\"2011-09-30T11:19:22.000Z\"},{\"id\":\"ping.telemetry@mozilla.com\",\"appDisabled\":false,\"version\":\"0.5\",\"installDate\":\"2011-11-16T14:53:20.000Z\",\"updateDate\":\"2011-11-16T20:15:43.000Z\"},{\"id\":\"aboutmetrics@mozilla.com\",\"appDisabled\":false,\"version\":\"0.1\",\"installDate\":\"2011-11-16T14:49:51.000Z\",\"updateDate\":\"2011-11-17T19:12:05.000Z\"},{\"id\":\"{972ce4c6-7e08-4474-a285-3208198ce6fd}\",\"appDisabled\":false,\"version\":\"11.0a1\",\"installDate\":\"2011-10-04T13:38:52.000Z\",\"updateDate\":\"2011-11-17T20:09:10.000Z\"},{\"id\":\"{e2c52c1c-5ee1-cc23-15fa-35945fd58806}\",\"appDisabled\":false,\"version\":\"1.0.0.0\",\"installDate\":\"2011-11-16T20:46:39.000Z\",\"updateDate\":\"2011-11-16T20:46:39.000Z\"},{\"id\":\"{ed5c33eb-95c1-f1be-50ba-eb0ade42d912}\",\"appDisabled\":false,\"version\":\"\",\"installDate\":\"2011-11-14T12:43:59.000Z\",\"updateDate\":\"2011-11-14T12:43:59.000Z\"},{\"id\":\"{bab155fa-7f14-a840-a791-88788942af10}\",\"appDisabled\":false,\"version\":\"\",\"installDate\":\"2011-11-09T00:50:33.000Z\",\"updateDate\":\"2011-11-09T00:50:33.000Z\"},{\"id\":\"{4a8fc06a-d958-67d5-1a6f-75e9a477dae8}\",\"appDisabled\":false,\"version\":\"\",\"installDate\":\"2011-10-21T17:14:47.000Z\",\"updateDate\":\"2011-10-21T17:14:47.000Z\"},{\"id\":\"{2b2bccd3-d699-b7ef-5644-15b419ff0c35}\",\"appDisabled\":false,\"version\":\"\",\"installDate\":\"2011-10-21T17:14:46.000Z\",\"updateDate\":\"2011-10-21T17:14:46.000Z\"},{\"id\":\"{f2d261dc-c5c4-ca3c-ae02-ccb3ff227c7f}\",\"appDisabled\":false,\"version\":\"\",\"installDate\":\"2011-03-15T19:50:22.000Z\",\"updateDate\":\"2011-03-15T19:50:22.000Z\"},{\"id\":\"{930f22c9-bb65-a324-c168-eded5a74647e}\",\"appDisabled\":false,\"version\":\"\",\"installDate\":\"2011-03-15T19:50:22.000Z\",\"updateDate\":\"2011-03-15T19:50:22.000Z\"},{\"id\":\"{f8708e87-9ec5-fb45-4a70-192ffed353e8}\",\"appDisabled\":false,\"version\":\"\",\"installDate\":\"2011-03-15T19:50:22.000Z\",\"updateDate\":\"2011-03-15T19:50:22.000Z\"},{\"id\":\"{729adf3c-ac1b-d74c-e059-6f6b5713340d}\",\"appDisabled\":false,\"version\":\"\",\"installDate\":\"2011-03-15T19:50:22.000Z\",\"updateDate\":\"2011-03-15T19:50:22.000Z\"}]},\"events\":{\"search\":{\"abouthome\":{\"Google\":2}},\"sessions\":{\"completedSessions\":27,\"completedSessionTime\":18867,\"completedSessionActiveTime\":730,\"abortedSessions\":1,\"abortedSessionTime\":62,\"abortedSessionActiveTime\":30,\"abortedSessionAvg\":62,\"abortedSessionMed\":62,\"currentSessionActiveTime\":20,\"currentSessionTime\":30,\"aboutSessionRestoreStarts\":0},\"corruptedEvents\":0}}";
    	String newDocument2 = "{\"ver\":1,\"uuid\":\"e8a583fe-98ec-45be-9e44-96a23759067a\",\"lastPingTime\":\"2011-11-17T20:10:00.000Z\",\"thisPingTime\":\"2011-11-28T14:22:07.223Z\",\"currentTime\":\"2011-11-28T14:22:12.232Z\",\"env\":{\"reason\":\"idle-daily\",\"OS\":\"Linux\",\"appID\":\"{ec8030f7-c20a-464f-9b0e-13a3a9e97384}\",\"appVersion\":\"11.0a1\",\"appVendor\":\"Mozilla\",\"appName\":\"Firefox\",\"appBuildID\":\"20111125135518\",\"appABI\":\"x86_64-gcc3\",\"appUpdateChannel\":\"default\",\"appDistribution\":\"default\",\"appDistributionVersion\":\"default\",\"platformBuildID\":\"20111125135518\",\"platformVersion\":\"11.0a1\",\"locale\":\"en-US\",\"name\":\"Linux\",\"version\":\"2.6.38-12-generic\",\"cpucount\":4,\"memsize\":7889,\"arch\":\"x86-64\"},\"simpleMeasurements\":{\"uptime\":1,\"main\":190,\"firstPaint\":1791,\"sessionRestored\":1575,\"isDefaultBrowser\":false,\"crashCountSubmitted\":0,\"profileAge\":55,\"placesPagesCount\":1535,\"placesBookmarksCount\":75,\"addonCount\":13,\"addons\":[{\"id\":\"crashme@ted.mielczarek.org\",\"appDisabled\":false,\"version\":\"0.3\",\"installDate\":\"2011-10-25T15:02:03.000Z\",\"updateDate\":\"2011-10-25T15:02:03.000Z\"},{\"id\":\"ping.telemetry@mozilla.com\",\"appDisabled\":false,\"version\":\"0.5\",\"installDate\":\"2011-11-16T14:53:20.000Z\",\"updateDate\":\"2011-11-16T20:15:43.000Z\"},{\"id\":\"aboutmetrics@mozilla.com\",\"appDisabled\":false,\"version\":\"0.1\",\"installDate\":\"2011-11-16T14:49:51.000Z\",\"updateDate\":\"2011-11-24T15:56:14.000Z\"},{\"id\":\"{972ce4c6-7e08-4474-a285-3208198ce6fd}\",\"appDisabled\":false,\"version\":\"11.0a1\",\"installDate\":\"2011-10-04T13:38:52.000Z\",\"updateDate\":\"2011-11-25T18:10:53.000Z\"},{\"id\":\"{e2c52c1c-5ee1-cc23-15fa-35945fd58806}\",\"appDisabled\":false,\"version\":\"1.0.0.0\",\"installDate\":\"2011-11-22T21:18:06.000Z\",\"updateDate\":\"2011-11-22T21:18:06.000Z\"},{\"id\":\"{18965679-bddd-de62-52b4-b56e6316d854}\",\"appDisabled\":false,\"version\":\"\",\"installDate\":\"2011-11-17T00:07:20.000Z\",\"updateDate\":\"2011-11-17T00:07:20.000Z\"},{\"id\":\"{6ccc47ab-3f2f-146f-82ea-c5dffb965818}\",\"appDisabled\":false,\"version\":\"\",\"installDate\":\"2011-11-17T00:07:20.000Z\",\"updateDate\":\"2011-11-17T00:07:20.000Z\"},{\"id\":\"{ed5c33eb-95c1-f1be-50ba-eb0ade42d912}\",\"appDisabled\":false,\"version\":\"\",\"installDate\":\"2011-11-14T12:43:59.000Z\",\"updateDate\":\"2011-11-14T12:43:59.000Z\"},{\"id\":\"{bab155fa-7f14-a840-a791-88788942af10}\",\"appDisabled\":false,\"version\":\"\",\"installDate\":\"2011-11-09T00:50:33.000Z\",\"updateDate\":\"2011-11-09T00:50:33.000Z\"},{\"id\":\"{f2d261dc-c5c4-ca3c-ae02-ccb3ff227c7f}\",\"appDisabled\":false,\"version\":\"\",\"installDate\":\"2011-03-15T19:50:22.000Z\",\"updateDate\":\"2011-03-15T19:50:22.000Z\"},{\"id\":\"{930f22c9-bb65-a324-c168-eded5a74647e}\",\"appDisabled\":false,\"version\":\"\",\"installDate\":\"2011-03-15T19:50:22.000Z\",\"updateDate\":\"2011-03-15T19:50:22.000Z\"},{\"id\":\"{f8708e87-9ec5-fb45-4a70-192ffed353e8}\",\"appDisabled\":false,\"version\":\"\",\"installDate\":\"2011-03-15T19:50:22.000Z\",\"updateDate\":\"2011-03-15T19:50:22.000Z\"},{\"id\":\"{729adf3c-ac1b-d74c-e059-6f6b5713340d}\",\"appDisabled\":false,\"version\":\"\",\"installDate\":\"2011-03-15T19:50:22.000Z\",\"updateDate\":\"2011-03-15T19:50:22.000Z\"}]},\"events\":{\"search\":{\"abouthome\":{\"Google\":1}},\"sessions\":{\"completedSessions\":37,\"completedSessionTime\":5946,\"completedSessionActiveTime\":1355,\"abortedSessions\":0,\"abortedSessionTime\":0,\"abortedSessionActiveTime\":0,\"currentSessionActiveTime\":10,\"currentSessionTime\":30,\"aboutSessionRestoreStarts\":0},\"corruptedEvents\":0}}";

//    	String expectedDocument1 = "{\"pingTime\":[\"2011-11-17T20:09:59.515Z\"],\"clockSkew\":[500],\"env\":{\"OS\":\"Linux\",\"appID\":\"{ec8030f7-c20a-464f-9b0e-13a3a9e97384}\",\"appVersion\":\"11.0a1\",\"appVendor\":\"Mozilla\",\"appName\":\"Firefox\",\"appBuildID\":\"20111117160821\",\"appABI\":\"x86_64-gcc3\",\"appUpdateChannel\":\"default\",\"appDistribution\":\"default\",\"appDistributionVersion\":\"default\",\"platformBuildID\":\"20111116163634\",\"platformVersion\":\"11.0a1\",\"locale\":\"en-US\",\"name\":\"Linux\",\"version\":\"2.6.38-12-generic\",\"cpucount\":4,\"memsize\":7889,\"arch\":\"x86-64\"},\"versions\":[[\"2011-11-17T20:09:59.515Z\",\"11.0a1\"]],\"simpleMeasurements\":{\"uptime\":{\"range\":[X,Y],\"bucket_count\":Z,\"histogram_type\":H,\"values\":{A:0, B:1, C:0},\"sum\":0},\"main\":[TODO:histogram],\"firstPaint\":[TODO:histogram],\"sessionRestored\":[TODO:histogram],\"isDefaultBrowser\":false,\"crashCountSubmitted\":[TODO:histogram],\"profileAge\":44,\"placesPagesCount\":[1202],\"placesBookmarksCount\":[57],\"addonCount\":[14],\"addons\":[ TODO:... ], // replace with latest},\"events\":{\"search\":{\"total\":{\"Amazon.com\":0,\"Bing\":0,\"Google\":2,\"Yahoo\":0,\"Other\":0},\"searchbar\":{\"Amazon.com\":[0],\"Bing\":[0],\"Google\":[0],\"Yahoo\":[0],\"Other\":[0]},\"urlbar\":{\"Amazon.com\":[0],\"Bing\":[0],\"Google\":[0],\"Yahoo\":[0],\"Other\":[0]},\"abouthome\":{\"Amazon.com\":[0],\"Bing\":[0],\"Google\":[2],\"Yahoo\":[0],\"Other\":[0]},\"contextmenu\":{\"Amazon.com\":[0],\"Bing\":[0],\"Google\":[0],\"Yahoo\":[0],\"Other\":[0]}},\"sessions\":{\"completedSessions\":[27],\"completedSessionTime\":[18867], completedSessionActivityRatio\":[(730/18867)], abortedSessions\":[1], abortedSessionTime\":[62], abortedSessionActivityRatio\":[(30/62)], abortedSessionMed\":[62], currentSessionTime\":[30], currentSessionActivityRatio\":[(20/30)], aboutSessionRestoreStarts\":[0]}, corruptedEvents\":[0]},\"uuid\":\"e8a583fe-98ec-45be-9e44-96a23759067a\"}";
    	String expectedDocument1 = "{\"pingTime\":[\"2011-11-17T20:09:59.515Z\"],\"clockSkew\":[500],\"env\":{\"OS\":\"Linux\",\"appID\":\"{ec8030f7-c20a-464f-9b0e-13a3a9e97384}\",\"appVersion\":\"11.0a1\",\"appVendor\":\"Mozilla\",\"appName\":\"Firefox\",\"appBuildID\":\"20111117160821\",\"appABI\":\"x86_64-gcc3\",\"appUpdateChannel\":\"default\",\"appDistribution\":\"default\",\"appDistributionVersion\":\"default\",\"platformBuildID\":\"20111116163634\",\"platformVersion\":\"11.0a1\",\"locale\":\"en-US\",\"name\":\"Linux\",\"version\":\"2.6.38-12-generic\",\"cpucount\":4,\"memsize\":7889,\"arch\":\"x86-64\"},\"versions\":[[\"2011-11-17T20:09:59.515Z\",\"11.0a1\"]],\"simpleMeasurements\":{},\"events\":{\"search\":{\"searchbar\":{\"Amazon.com\":[0],\"Bing\":[0],\"Google\":[0],\"Yahoo\":[0],\"Other\":[0]},\"urlbar\":{\"Amazon.com\":[0],\"Bing\":[0],\"Google\":[0],\"Yahoo\":[0],\"Other\":[0]},\"abouthome\":{\"Amazon.com\":[0],\"Bing\":[0],\"Google\":[2],\"Yahoo\":[0],\"Other\":[0]},\"contextmenu\":{\"Amazon.com\":[0],\"Bing\":[0],\"Google\":[0],\"Yahoo\":[0],\"Other\":[0]},\"total\":{\"Amazon.com\":0,\"Bing\":0,\"Google\":2,\"Yahoo\":0,\"Other\":0}},\"sessions\":{\"completedSessions\":[27],\"completedSessionTime\":[18867],\"completedSessionActivityRatio\":[0.038691895902899245],\"abortedSessions\":[1],\"abortedSessionTime\":[62],\"abortedSessionActivityRatio\":[0.4838709677419355],\"abortedSessionMed\":[62],\"currentSessionTime\":[30],\"currentSessionActivityRatio\":[0.6666666666666666],\"aboutSessionRestoreStarts\":[0]},\"corruptedEvents\":[0]},\"uuid\":\"e8a583fe-98ec-45be-9e44-96a23759067a\"}";
		
        metricsPingPreCommit.setReferenceDate("2011-11-17T20:10:00.015Z");
    	String aggregated1 = metricsPingPreCommit.preCommit(mapName, id, newDocument1);
    	System.out.println("Aggregate 1: " + aggregated1);
    	System.out.println("Expected 1:  " + expectedDocument1);
    	assertEquals(expectedDocument1, aggregated1);
    	
    	metricsPingPreCommit.setReferenceDate("2011-11-28T14:22:07.673Z");
    	// Apply the next one:
//    	String expectedDocument2 = "{\"pingTime\":[\"2011-11-17T20:09:59.515Z\",\"2011-11-28T14:22:07.223Z\"],\"clockSkew\":[500,450],\"env\":{\"OS\":\"Linux\",\"appID\":\"{ec8030f7-c20a-464f-9b0e-13a3a9e97384}\",\"appVersion\":\"11.0a1\",\"appVendor\":\"Mozilla\",\"appName\":\"Firefox\",\"appBuildID\":\"20111125135518\",\"appABI\":\"x86_64-gcc3\",\"appUpdateChannel\":\"default\",\"appDistribution\":\"default\",\"appDistributionVersion\":\"default\",\"platformBuildID\":\"20111125135518\",\"platformVersion\":\"11.0a1\",\"locale\":\"en-US\",\"name\":\"Linux\",\"version\":\"2.6.38-12-generic\",\"cpucount\":4,\"memsize\":7889,\"arch\":\"x86-64\"},\"versions\":[[\"2011-11-17T20:09:59.515Z\",\"11.0a1\"]],\"simpleMeasurements\":{\"uptime\":{\"range\":[X,Y],\"bucket_count\":Z,\"histogram_type\":H,\"values\":{A:0,B:2,C:0},\"sum\":0},\"main\":[TODO:histogram],\"firstPaint\":[TODO:histogram],\"sessionRestored\":[TODO:histogram],\"isDefaultBrowser\":false,\"crashCountSubmitted\":[TODO:histogram],\"profileAge\":55,\"placesPagesCount\":[1202,1535],\"placesBookmarksCount\":[57,75],\"addonCount\":[14,13],\"addons\":[ TODO:... ],},\"events\":{\"search\":{\"total\":{\"Amazon.com\":0,\"Bing\":0,\"Google\":3,\"Yahoo\":0,\"Other\":0},\"searchbar\":{\"Amazon.com\":[0,0],\"Bing\":[0,0],\"Google\":[0,0],\"Yahoo\":[0,0],\"Other\":[0,0]},\"urlbar\":{\"Amazon.com\":[0,0],\"Bing\":[0,0],\"Google\":[0,0],\"Yahoo\":[0,0],\"Other\":[0,0]},\"abouthome\":{\"Amazon.com\":[0,0],\"Bing\":[0,0],\"Google\":[2,1],\"Yahoo\":[0,0],\"Other\":[0,0]},\"contextmenu\":{\"Amazon.com\":[0,0],\"Bing\":[0,0],\"Google\":[0,0],\"Yahoo\":[0,0],\"Other\":[0,0]}},\"sessions\":{\"completedSessions\":[27,37],\"completedSessionTime\":[18867,5946],\"completedSessionActivityRatio\":[(730/18867),(1355/5946)],\"abortedSessions\":[1,0],\"abortedSessionTime\":[62,0],\"abortedSessionActivityRatio\":[(30/62),0],\"abortedSessionMed\":[62,0],\"currentSessionTime\":[30,30],\"currentSessionActivityRatio\":[(20/30), (10/30)],\"aboutSessionRestoreStarts\":[0,0]},\"corruptedEvents\":[0,0]},\"uuid\":\"e8a583fe-98ec-45be-9e44-96a23759067a\"}";
    	String expectedDocument2 = "{\"pingTime\":[\"2011-11-17T20:09:59.515Z\",\"2011-11-28T14:22:07.223Z\"],\"clockSkew\":[500,450],\"env\":{\"OS\":\"Linux\",\"appID\":\"{ec8030f7-c20a-464f-9b0e-13a3a9e97384}\",\"appVersion\":\"11.0a1\",\"appVendor\":\"Mozilla\",\"appName\":\"Firefox\",\"appBuildID\":\"20111125135518\",\"appABI\":\"x86_64-gcc3\",\"appUpdateChannel\":\"default\",\"appDistribution\":\"default\",\"appDistributionVersion\":\"default\",\"platformBuildID\":\"20111125135518\",\"platformVersion\":\"11.0a1\",\"locale\":\"en-US\",\"name\":\"Linux\",\"version\":\"2.6.38-12-generic\",\"cpucount\":4,\"memsize\":7889,\"arch\":\"x86-64\"},\"versions\":[[\"2011-11-17T20:09:59.515Z\",\"11.0a1\"]],\"simpleMeasurements\":{},\"events\":{\"search\":{\"searchbar\":{\"Amazon.com\":[0,0],\"Bing\":[0,0],\"Google\":[0,0],\"Yahoo\":[0,0],\"Other\":[0,0]},\"urlbar\":{\"Amazon.com\":[0,0],\"Bing\":[0,0],\"Google\":[0,0],\"Yahoo\":[0,0],\"Other\":[0,0]},\"abouthome\":{\"Amazon.com\":[0,0],\"Bing\":[0,0],\"Google\":[2,1],\"Yahoo\":[0,0],\"Other\":[0,0]},\"contextmenu\":{\"Amazon.com\":[0,0],\"Bing\":[0,0],\"Google\":[0,0],\"Yahoo\":[0,0],\"Other\":[0,0]},\"total\":{\"Amazon.com\":0,\"Bing\":0,\"Google\":3,\"Yahoo\":0,\"Other\":0}},\"sessions\":{\"completedSessions\":[27,37],\"completedSessionTime\":[18867,5946],\"completedSessionActivityRatio\":[0.038691895902899245,0.22788429196098217],\"abortedSessions\":[1,0],\"abortedSessionTime\":[62,0],\"abortedSessionActivityRatio\":[0.4838709677419355,0.0],\"abortedSessionMed\":[62,0],\"currentSessionTime\":[30,30],\"currentSessionActivityRatio\":[0.6666666666666666,0.3333333333333333],\"aboutSessionRestoreStarts\":[0,0]},\"corruptedEvents\":[0,0]},\"uuid\":\"e8a583fe-98ec-45be-9e44-96a23759067a\"}";
    	Map<String, String> m = Hazelcast.getMap(mapName);
        m.put(id, aggregated1);
    	String aggregated2 = metricsPingPreCommit.preCommit(mapName, id, newDocument2);
    	System.out.println("Aggregate 2: " + aggregated2);
    	System.out.println("Expected 2:  " + expectedDocument2);
    	assertEquals(expectedDocument2, aggregated2);
    }
}
