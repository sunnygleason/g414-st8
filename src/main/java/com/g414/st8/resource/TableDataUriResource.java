package com.g414.st8.resource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import org.codehaus.jackson.map.ObjectMapper;

import com.g414.st8.inno.DataManager;
import com.google.inject.Inject;
import com.sun.jersey.api.uri.UriComponent;

@Path("/d")
@Produces(MediaType.APPLICATION_JSON)
public class TableDataUriResource {
    @Inject
    private DataManager manager;

    @Inject
    private ObjectMapper mapper;

    @GET
    @Path("{tablename}")
    public String getData(@PathParam("tablename") String tableName,
            @Context UriInfo uri) throws Exception {
        return mapper.writeValueAsString(manager.loadData(tableName,
                convertUriToMap(uri.getPath())));
    }

    @PUT
    @Path("{tablename}")
    public void insertData(@PathParam("tablename") String tableName,
            @Context UriInfo uri) throws Exception {
        manager.insertData(tableName, convertUriToMap(uri.getPath()));
    }

    @POST
    @Path("{tablename}")
    public void updateData(@PathParam("tablename") String tableName,
            @Context UriInfo uri) throws Exception {
        manager.updateData(tableName, convertUriToMap(uri.getPath()));
    }

    @DELETE
    @Path("{tablename}")
    public void deleteData(@PathParam("tablename") String tableName,
            @Context UriInfo uri) throws Exception {
        manager.deleteData(tableName, convertUriToMap(uri.getPath()));
    }

    private static Map<String, Object> convertUriToMap(String path) {
        return flattenMap(UriComponent.decodeMatrix(path, true));
    }

    private static Map<String, Object> flattenMap(
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
