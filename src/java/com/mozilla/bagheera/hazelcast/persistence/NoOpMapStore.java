package com.mozilla.bagheera.hazelcast.persistence;

import java.util.Collection;
import java.util.Map;

import org.apache.log4j.Logger;

import com.hazelcast.core.MapStore;

public class NoOpMapStore implements MapStore<String, String> {
    private static final Logger logger = Logger.getLogger(HBaseMapStore.class);

	@Override
	public String load(String key) {
		logger.info(String.format("load called\nkey: %s", key));
		return null;
	}

	@Override
	public Map<String, String> loadAll(Collection<String> keys) {
		logger.info(String.format("loadAll called with %d keys", keys.size()));
		return null;
	}

	@Override
	public void store(String key, String value) {
		logger.info(String.format("store called\nkey: %s\nvalue: %s", key, value));
	}

	@Override
	public void storeAll(Map<String, String> map) {
		logger.info(String.format("storeAll called with %d entries", map.size()));
	}

	@Override
	public void delete(String key) {
		logger.info(String.format("delete called\nkey: %s", key));
	}

	@Override
	public void deleteAll(Collection<String> keys) {
		logger.info(String.format("storeAll called with %d entries", keys.size()));
	}

}
