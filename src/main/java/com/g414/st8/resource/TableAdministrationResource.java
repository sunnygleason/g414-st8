package com.g414.st8.resource;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.map.ObjectMapper;

import com.g414.st8.haildb.TableManager;
import com.g414.st8.model.TableDefinition;
import com.google.inject.Inject;

@Path("/1.0/t")
public class TableAdministrationResource {
    @Inject
    private TableManager manager;

    @Inject
    private ObjectMapper mapper;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("id")
    public String listAllTables() throws Exception {
        return mapper.writeValueAsString(manager.listAllTables());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("name")
    public String listAllTablesByName() throws Exception {
        return mapper.writeValueAsString(manager.listAllTablesByName());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{tablename}")
    public TableDefinition getTable(@PathParam("tablename") String name)
            throws Exception {
        TableDefinition result = manager.getTable(name, false);

        if (result == null) {
            throw new WebApplicationException(HttpServletResponse.SC_NOT_FOUND);
        }

        return result;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{tablename}")
    public String createTable(@PathParam("tablename") String name,
            TableDefinition tableDefinition) throws Exception {
        manager.createTable(name, tableDefinition);

        Map<String, String> result = new LinkedHashMap<String, String>();
        result.put("status", "OK");

        return mapper.writeValueAsString(result);
    }

    @POST
    @Path("{tablename}/truncate")
    public void truncateTable(@PathParam("tablename") String name)
            throws Exception {
        manager.truncateTable(name);
    }

    @DELETE
    @Path("{tablename}")
    public void deleteTable(@PathParam("tablename") String name)
            throws Exception {
        manager.deleteTable(name);
    }
}
