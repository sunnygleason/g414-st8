package com.g414.st8.resource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.codehaus.jackson.map.ObjectMapper;

import com.g414.st8.inno.DataManager;
import com.google.inject.Inject;

@Path("/f")
@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
@Produces(MediaType.APPLICATION_JSON)
public class TableDataFormResource {
    @Inject
    private DataManager manager;

    @Inject
    private ObjectMapper mapper;

    @GET
    @Path("{tablename}")
    public String getFormData(@PathParam("tablename") String tableName,
            MultivaluedMap<String, String> inForm) throws Exception {
        return mapper.writeValueAsString(manager.loadData(tableName,
                flattenMap(inForm)));
    }

    @PUT
    @Path("{tablename}")
    public void insertFormData(@PathParam("tablename") String tableName,
            MultivaluedMap<String, String> inForm) throws Exception {
        manager.insertData(tableName, flattenMap(inForm));
    }

    @POST
    @Path("{tablename}")
    public void updateFormData(@PathParam("tablename") String tableName,
            MultivaluedMap<String, String> inForm) throws Exception {
        manager.updateData(tableName, flattenMap(inForm));
    }

    @DELETE
    @Path("{tablename}")
    public void deleteFormData(@PathParam("tablename") String tableName,
            MultivaluedMap<String, String> inForm) throws Exception {
        manager.deleteData(tableName, flattenMap(inForm));
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
