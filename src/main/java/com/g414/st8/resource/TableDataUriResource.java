package com.g414.st8.resource;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import org.codehaus.jackson.map.ObjectMapper;

import com.g414.st8.haildb.DataManager;
import com.google.inject.Inject;

@Path("/1.0/d")
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
                ResourceHelper.convertUriToMap(uri.getPath())));
    }

    @POST
    @Path("{tablename}")
    public void insertData(@PathParam("tablename") String tableName,
            @Context UriInfo uri) throws Exception {
        manager.insertData(tableName, ResourceHelper.convertUriToMap(uri
                .getPath()));
    }

    @PUT
    @Path("{tablename}")
    public void updateData(@PathParam("tablename") String tableName,
            @Context UriInfo uri) throws Exception {
        manager.updateData(tableName, ResourceHelper.convertUriToMap(uri
                .getPath()));
    }

    @DELETE
    @Path("{tablename}")
    public void deleteData(@PathParam("tablename") String tableName,
            @Context UriInfo uri) throws Exception {
        manager.deleteData(tableName, ResourceHelper.convertUriToMap(uri
                .getPath()));
    }
}
