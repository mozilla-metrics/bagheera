package com.mozilla.bagheera.hazelcast.persistence;

import java.util.HashMap;
import java.util.Map;

import com.hazelcast.core.MapStore;

public class MapStoreRepository {

    private static Map<String,MapStore<String,String>> instances = new HashMap<String,MapStore<String,String>>();

    public static void addMapStore(String mapName, MapStore<String,String> mapStore) {
        instances.put(mapName, mapStore);
    }
    
    public static MapStore<String,String> getMapStore(String mapName) {
        return instances.get(mapName);
    }
    
}
