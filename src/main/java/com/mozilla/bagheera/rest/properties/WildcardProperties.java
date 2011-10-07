package com.mozilla.bagheera.rest.properties;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WildcardProperties extends Properties {

    private static final long serialVersionUID = 8726833938438520686L;
    
    private final Pattern propertyPattern = Pattern.compile("^([^\\.]+)\\.(.*)");;
    
    /**
     * Get a property if it exists. If not check for wildcard property matches.
     * 
     * @param name
     * @param defaultValue
     * @return
     */
    public String getWildcardProperty(String name, String defaultValue) {
        String v = null;
        if (containsKey(name)) {
            v = getProperty(name);
        } else {
            for (Object k : keySet()) {
                String ks = (String)k;
                Matcher m = propertyPattern.matcher(ks);
                if (m.find() && m.groupCount() == 2) {
                    String propMapName = m.group(1);
                    if (propMapName.contains("*")) {
                        Pattern propPattern = Pattern.compile(ks.replaceAll("\\*", ".+"));
                        Matcher m2 = propPattern.matcher(name);
                        if (m2.find()) {
                            v = getProperty(ks);
                            break;
                        }
                    }
                }
            }
        }
        
        if (v == null) {
            v = defaultValue;
        } else {
            setProperty(name, v);
        }
        
        return v;
    }
    
}
