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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.hazelcast.core.Hazelcast;


/**
 * Utility class for aggregating incoming pings with existing documents.
 */
public class MetricsPingPreCommit implements PreCommitHook {
    private static final Logger LOG = Logger.getLogger(MetricsPingPreCommit.class);
    
	private ObjectMapper objectMapper;
	private static String[] searchEngineBuckets = {"Amazon.com", "Bing", "Google", "Yahoo", "Other"};
	private static String[] searchSources = {"searchbar", "urlbar", "abouthome", "contextmenu"};
	private static String[] sessionKeys = {"completedSessions", "completedSessionTime", 
		"completedSessionActivityRatio", "abortedSessions", "abortedSessionTime", 
		"abortedSessionActivityRatio", "abortedSessionMed", "currentSessionTime", 
		"currentSessionActivityRatio", "aboutSessionRestoreStarts"};
	private static String[] simpleMeasureKeys = {"uptime", "main", "firstPaint", 
			"sessionRestored", "isDefaultBrowser", "crashCountSubmitted",
			"crashCountPending", "profileAge", "placesPagesCount",
			"placesBookmarksCount", "addonCount"};
	private static String[] envKeys = {"OS", "appID", "appVersion", "appVendor", "appName", 
       "appBuildID", "appABI", "appUpdateChannel", "appDistribution",
       "appDistributionVersion", "platformBuildID", "platformVersion",
       "locale","name","version","cpucount","memsize","arch"};

	
	private SimpleDateFormat dateFormat;
	
	private Date referenceDate = new Date();
	
	public MetricsPingPreCommit() {
		objectMapper = new ObjectMapper();
		dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		//explicitly set timezone
		dateFormat.setTimeZone(java.util.TimeZone.getTimeZone("Zulu"));
	}
	
	@Override
	public String preCommit(String mapName, String id, String newDocument) {// throws JsonParseException, JsonMappingException, IOException {
		String result = null;

		try {
			result = preCommitWithExceptions(mapName, id, newDocument);
		} catch (JsonParseException e) {
			LOG.error("Error parsing JSON Document", e);
		} catch (JsonMappingException e) {
			LOG.error("Error mapping JSON Document", e);
		} catch (IOException e) {
			LOG.error("Error reading JSON Document", e);
		}

		return result;
	}
	
	public String preCommitWithExceptions(String mapName, String id, String newDocument) throws JsonParseException, JsonMappingException, IOException {
		// 1. Retrieve existing document by id (to be extracted to a base class)
		Map<String, String> m = Hazelcast.getMap(mapName);
        String existingDocument = m.get(id);
        JsonNode aggregate;
        if (existingDocument == null) {
        	// This is the first submission for the given ID
        	aggregate = createEmptyAggregate();
        } else {
        	// Time to merge with the previous document.
        	aggregate = objectMapper.readValue(existingDocument, JsonNode.class);
        }
        
        JsonNode incoming = objectMapper.readValue(newDocument, JsonNode.class);

		// 2. Apply incoming json to document
        applyToAggregate(aggregate, incoming);
        
		// 3. Return the aggregated document as a String
        return objectMapper.writeValueAsString(aggregate);
	}

	private void applyToAggregate(JsonNode uncheckedAggregate, JsonNode uncheckedIncoming) {
		if (!isValidAggregate(uncheckedAggregate)) {
			LOG.error("Invalid merged document");
			throw new IllegalArgumentException("Invalid aggregate");
		}
		
		if (!isValidInstance(uncheckedIncoming)) {
			LOG.error("Invalid incoming document");
			throw new IllegalArgumentException("Invalid instance");
		}
		
		ObjectNode aggregate = (ObjectNode)uncheckedAggregate;
		ObjectNode incoming = (ObjectNode)uncheckedIncoming;
		
		aggregate.put("uuid", incoming.get("uuid").getTextValue());

		String pingTimeText = incoming.get("thisPingTime").getTextValue();
		ArrayNode pingTime = (ArrayNode)aggregate.get("pingTime");
		pingTime.add(pingTimeText);

		// Also calculate skew and duration
		ArrayNode clockSkewNode = (ArrayNode)aggregate.get("clockSkew");
		ArrayNode pingDurationNode = (ArrayNode)aggregate.get("pingDuration");
		Date parsedPingTime = parseDateString(pingTimeText);
		Date parsedLastPingTime = parseDateString(incoming.get("lastPingTime").getTextValue());
		
		// TODO: if "lastPingTime" doesn't match with the existing aggregate, we may have
		//       a situation where the same UUID is being used by multiple profiles, such
		//       as when a profile is copied to a new computer, and the copy and the
		//       original both continue to submit.
		//       In that case, we should apply the current submission to a new UUID and 
		//       assign that UUID to the client.
		
		if (parsedPingTime != null) {
			long clockSkew = (getReferenceDate().getTime() - parsedPingTime.getTime());
			clockSkewNode.add(clockSkew);
			
			if (parsedLastPingTime != null) {
				long pingDurationSeconds = (parsedPingTime.getTime() - parsedLastPingTime.getTime()) / 1000;
				pingDurationNode.add(pingDurationSeconds);
			} else {
				LOG.error("Could not parse 'lastPingTime' from incoming document - failed to calculate pingDuration");
				pingDurationNode.add("NO_DATA");
			}
		} else {
			LOG.error("Could not parse 'thisPingTime' from incoming document - failed to calculate clockSkew and pingDuration");
			clockSkewNode.add("NO_DATA");
		}

		String currentVersionText = null;
		// capture latest interesting "env" keys
		// TODO: should we empty out merged "env" first in case of deprecated env keys?
		ObjectNode env = (ObjectNode)aggregate.get("env");
		ObjectNode envIn = (ObjectNode)incoming.get("env");
		for (String envKey : envKeys) {
			if (envIn.has(envKey)) {
				env.put(envKey, envIn.get(envKey));
				
				// grab this for later
				if (envKey.equals("appVersion")) {
					currentVersionText = envIn.get(envKey).getTextValue();
				}
			} else {
				LOG.warn("Missing important key " + envKey);
			}
		}
				
		// Check if the version has changed:
		if (currentVersionText != null) {
			ArrayNode versionList = (ArrayNode)aggregate.get("versions");
			int numVersions = versionList.size();
			boolean shouldAppend = false;
			if (numVersions > 0) {
				// Get the last one:
				JsonNode lastVersion = versionList.get(numVersions - 1);
				if (!lastVersion.isMissingNode() && lastVersion.isArray()) {
					JsonNode lastVersionNum = lastVersion.get(1);
					if (!lastVersionNum.isMissingNode() && lastVersionNum.isValueNode()) {
						if (!currentVersionText.equals(lastVersionNum.getTextValue())) {
							// At last!
							shouldAppend = true;
						}
					} else {
						LOG.error("Error fetching last known version");
					}
				}
			} else {
				// No versions yet, we should append
				shouldAppend = true;
			}

			if (shouldAppend) {
				ArrayNode newVersion = objectMapper.createArrayNode();
				newVersion.add(pingTimeText);
				newVersion.add(currentVersionText);
				versionList.add(newVersion);
			}
		}
		
		// Simple measurements:
		ObjectNode simpleNode = (ObjectNode)aggregate.get("simpleMeasurements");
		ObjectNode simpleNodeIn = (ObjectNode)incoming.get("simpleMeasurements");
		aggregate.put("addons", simpleNodeIn.get("addons"));
		
		// TODO: setup defaults for each simpleMeasureKeys entry.  For example,
		//       crashCountPending -> 0
		//       uptime -> "NO_DATA" (?)
		//       We need to avoid having jagged arrays, otherwise we lose the
		//       ability to associate a value with a pingTime.
		for(String simpleKey : simpleMeasureKeys) {
			ArrayNode simpleArr = (ArrayNode)simpleNode.get(simpleKey);
			if (simpleNodeIn.has(simpleKey))
				simpleArr.add(simpleNodeIn.get(simpleKey));
			else
				simpleArr.add("NO_DATA");
		}

		// Process events:
		JsonNode eventNode = aggregate.get("events");
		JsonNode eventNodeIn = incoming.get("events");

		// Searches:
		JsonNode searchNode = eventNode.get("search");
		JsonNode searchNodeIn = eventNodeIn.get("search");
		if (searchNodeIn != null && searchNodeIn.isObject()) {
			ObjectNode total = (ObjectNode)searchNode.get("total");
			for (String searchSource : searchSources) {
				JsonNode source = searchNode.get(searchSource);
				JsonNode sourceIn = searchNodeIn.get(searchSource);
				for (String engine : searchEngineBuckets) {
					JsonNode engineNode = source.get(engine);
					JsonNode totalForEngine = total.get(engine);
					if (engineNode != null && engineNode.isArray()) {
						int searchCount = 0;
						if (sourceIn != null && sourceIn.has(engine)) {
							searchCount = sourceIn.get(engine).asInt(0);
						} else {
							// normal - no searches for this source/engine
							LOG.debug(String.format("No searches for source '%s', engine '%s'", searchSource, engine));
						}
						((ArrayNode)engineNode).add(searchCount);
						int totalCount = totalForEngine.asInt(0);
						totalCount += searchCount;
						total.put(engine, totalCount);
					} else {
						LOG.warn(String.format("Missing or corrupted value for merged source '%s', engine '%s' (expected an array)", searchSource, engine));
					}
				}
			}
		}

		// Sessions:
		JsonNode sessions = eventNode.get("sessions");
		JsonNode sessionsIn = eventNodeIn.get("sessions");
		
		// aside from ratios, everything is simple:
		String[] simpleKeys = new String[]{"completedSessions", "completedSessionTime", 
				"abortedSessions", "abortedSessionTime", "abortedSessionMed", "currentSessionTime", 
				"aboutSessionRestoreStarts"};
		for (String sessKey : simpleKeys) {
			JsonNode newSessionValue = sessionsIn.get(sessKey);
			int newSessionIntValue = 0;
			if (newSessionValue != null) {
				newSessionIntValue = newSessionValue.asInt(0);
			} else {
				// this is OK, we just didn't get a value
				LOG.debug(String.format("No incoming value for session key '%s'", sessKey));
			}
			JsonNode aggregateSessionValue = sessions.get(sessKey);
			if (aggregateSessionValue != null && aggregateSessionValue.isArray()) {
				((ArrayNode)aggregateSessionValue).add(newSessionIntValue);
			} else {
				// error - no aggregate.
				LOG.error(String.format("Missing or corrupted value for merged session key '%s'", sessKey));
			}
		}

		// Now do ratio ones.
		String[] ratioTypes = new String[]{"completed", "aborted", "current"};
		for (String ratioType : ratioTypes) {
			String totalField = ratioType + "SessionTime";
			String activeField = ratioType + "SessionActiveTime";
			String ratioField = ratioType + "SessionActivityRatio";
			int totalValue = 0;
			int activeValue = 0;
			if (sessionsIn.has(totalField)) {
				totalValue = sessionsIn.get(totalField).asInt(0);
			} else {
				// OK
				LOG.debug(String.format("No incoming value for total '%s'", totalField));
			}
			if (sessionsIn.has(activeField)) {
				activeValue = sessionsIn.get(activeField).asInt(0);
			} else {
				// OK
				LOG.debug(String.format("No incoming value for active '%s'", activeField));
			}

			double ratio = 0.0;
			if (totalValue > 0 && activeValue > 0) {
				ratio = (double)activeValue / (double)totalValue;
			}

			JsonNode ratioList = sessions.get(ratioField);
			if (ratioList != null && ratioList.isArray()) {
				((ArrayNode)ratioList).add(ratio);
			} else {
				LOG.warn(String.format("No merged value found for ratio '%s'", ratioField));
			}
				
		}

		// Corrupted Events:
		ArrayNode corruptedEvents = (ArrayNode)eventNode.get("corruptedEvents");
		int numCorruptedEvents = 0;

		JsonNode corruptedEventsIn = eventNodeIn.get("corruptedEvents");
		numCorruptedEvents = corruptedEventsIn.asInt(0);

		corruptedEvents.add(numCorruptedEvents);
	}

	private Date parseDateString(String dateText) {
		Date parsedDate = null;
		try {
			parsedDate = dateFormat.parse(dateText);
		} catch (ParseException e) {
			LOG.error("Error parsing client timestamp: " + dateText 
					+ " (expecting something of the form 2000-01-01T00:00:01.515Z)");
		}
		return parsedDate;
	}
	
	public Date getReferenceDate() {
		return referenceDate;
	}

	// Useful for testing / debugging - set a date which will be used to calculate
	// clock skew.
	public void setReferenceDate(Date referenceDate) {
		this.referenceDate = referenceDate;
	}
	
	// TODO: fix or remove this - used in testing
	public void setReferenceDate(String referenceDate) {
		Date d;
		try {
			d = dateFormat.parse(referenceDate);
			this.setReferenceDate(d);
		} catch (ParseException e) {
			LOG.error("Error parsing reference date", e);
		}
	}
	
	// Things common to aggregates and instances.
	protected boolean isValidAggregateOrInstance(JsonNode generic) {
		if (generic == null || !generic.isObject()) return false;
		
		JsonNode env = generic.get("env");
		if (env == null || !env.isObject()) return false;
		
		JsonNode simple = generic.get("simpleMeasurements");
		if (simple == null || !simple.isObject()) return false;
		
		JsonNode events = generic.get("events");
		if (events == null || !events.isObject()) return false;
		
		JsonNode sessions = events.get("sessions");
		if (sessions == null || !sessions.isObject()) return false;
		
		return true;
	}
	
	// Check if all specified nodeNames exist and are ArrayNode children
	// of the given document.
	private boolean isArrayChild(JsonNode document, String... nodeNames) {
		for (String nodeName : nodeNames) {
			JsonNode aNode = document.path(nodeName);
			if (!aNode.isArray())
				return false;
		}
		return true;
	}

	protected boolean isValidAggregate(JsonNode aggregate) {
		if (!isValidAggregateOrInstance(aggregate)) return false;
		if (aggregate == null || !aggregate.isObject()) return false;

		if (!isArrayChild(aggregate, "pingTime", "pingDuration", "clockSkew", "versions", "addons"))
			return false;
		
		JsonNode simple = aggregate.get("simpleMeasurements");
		if (!simple.isObject()) return false;
		
		if (!isArrayChild(simple, "uptime", "main", "firstPaint", "sessionRestored",
				"isDefaultBrowser", "crashCountSubmitted", "crashCountPending",
				"profileAge", "placesPagesCount", "placesBookmarksCount", "addonCount")) {
			return false;
		}
		
		// "events" checked for null in isValidAggregateOrInstance
		JsonNode events = aggregate.get("events");
		
		JsonNode corruptedEvents = events.path("corruptedEvents");
		if (!corruptedEvents.isArray()) return false;
		
		JsonNode search = events.path("search");
		if (!search.isObject()) return false;
		
		JsonNode searchTotal = search.path("total");
		if (!searchTotal.isObject()) return false;
		
		return true;
	}
	
	protected boolean isValidInstance(JsonNode instance) {
		if (!isValidAggregateOrInstance(instance)) return false;
		
		JsonNode uuid = instance.path("uuid");
		if (!uuid.isValueNode()) return false;

		JsonNode pingTime = instance.path("thisPingTime");
		if(!pingTime.isValueNode()) return false;
		
		JsonNode lastPingTime = instance.path("lastPingTime");
		if(!lastPingTime.isValueNode()) return false;

		// "events" checked for null in isValidAggregateOrInstance
		JsonNode events = instance.get("events");
		JsonNode corruptedEvents = events.path("corruptedEvents");
		if (!corruptedEvents.isValueNode()) return false;
		
		return true;
	}

	public JsonNode createEmptyAggregate() {
		ObjectNode root = objectMapper.createObjectNode();
//		root.put("uuid", "UNKNOWN");

		root.put("pingTime", objectMapper.createArrayNode());
		root.put("pingDuration", objectMapper.createArrayNode());
		root.put("clockSkew", objectMapper.createArrayNode());
		root.put("env", objectMapper.createObjectNode());
		root.put("versions", objectMapper.createArrayNode());
		root.put("addons", objectMapper.createArrayNode()); // This comes from simpleMeasurements
		
		ObjectNode simple = objectMapper.createObjectNode();
		simple.put("uptime", objectMapper.createArrayNode());
		simple.put("main", objectMapper.createArrayNode());
		simple.put("firstPaint", objectMapper.createArrayNode());
		simple.put("sessionRestored", objectMapper.createArrayNode());
		simple.put("isDefaultBrowser", objectMapper.createArrayNode());
		simple.put("crashCountSubmitted", objectMapper.createArrayNode());
		simple.put("crashCountPending", objectMapper.createArrayNode());
		simple.put("profileAge", objectMapper.createArrayNode());
		simple.put("placesPagesCount", objectMapper.createArrayNode());
		simple.put("placesBookmarksCount", objectMapper.createArrayNode());
		simple.put("addonCount", objectMapper.createArrayNode());
		root.put("simpleMeasurements", simple);

		ObjectNode events = objectMapper.createObjectNode();
		ObjectNode search = objectMapper.createObjectNode();
		ObjectNode sessions = objectMapper.createObjectNode();
		events.put("search", search);
		events.put("sessions", sessions);
		events.put("corruptedEvents", objectMapper.createArrayNode());
		root.put("events", events);
		
		for(String searchSource : searchSources) {
			ObjectNode aNode = objectMapper.createObjectNode();
			for(String engine : searchEngineBuckets) {
				aNode.put(engine, objectMapper.createArrayNode());
			}
			search.put(searchSource, aNode);
		}
		
		ObjectNode totalNode = objectMapper.createObjectNode();
		for(String engine : searchEngineBuckets) {
			totalNode.put(engine, 0);
		}
		search.put("total", totalNode);
		
		for(String sessionKey : sessionKeys) {
			sessions.put(sessionKey, objectMapper.createArrayNode());
		}
		
		return root;
	}
}