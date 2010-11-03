package com.g414.st8.resource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import com.sun.jersey.api.uri.UriComponent;

public class ResourceHelper {
    public static Map<String, Object> convertUriToMap(String path) {
        return flattenMap(UriComponent.decodeMatrix(path, true));
    }

    public static Map<String, Object> flattenMap(
            MultivaluedMap<String, String> inMap) {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        for (Map.Entry<String, List<String>> e : inMap.entrySet()) {
            String key = e.getKey();
            List<String> values = e.getValue();
            if (values.size() == 1) {
                result.put(key, values.get(0));
            } else {
                throw new IllegalArgumentException(
                        "Multivalued Parameters not allowed: " + key);
            }
        }

        return result;
    }
}
