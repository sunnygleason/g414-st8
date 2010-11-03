package com.g414.st8.resource;

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
                ResourceHelper.flattenMap(inForm)));
    }

    @PUT
    @Path("{tablename}")
    public void insertFormData(@PathParam("tablename") String tableName,
            MultivaluedMap<String, String> inForm) throws Exception {
        manager.insertData(tableName, ResourceHelper.flattenMap(inForm));
    }

    @POST
    @Path("{tablename}")
    public void updateFormData(@PathParam("tablename") String tableName,
            MultivaluedMap<String, String> inForm) throws Exception {
        manager.updateData(tableName, ResourceHelper.flattenMap(inForm));
    }

    @DELETE
    @Path("{tablename}")
    public void deleteFormData(@PathParam("tablename") String tableName,
            MultivaluedMap<String, String> inForm) throws Exception {
        manager.deleteData(tableName, ResourceHelper.flattenMap(inForm));
    }
}
