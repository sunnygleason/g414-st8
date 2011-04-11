package com.g414.st8.resource;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import org.codehaus.jackson.map.ObjectMapper;

import com.g414.haildb.Database;
import com.g414.st8.haildb.InternalTableDefinitions;
import com.g414.st8.haildb.TableManager;
import com.g414.st8.model.TableDefinition;
import com.google.inject.Inject;

@Path("/1.0/clear")
public class ClearDataResource {
    @Inject
    private Database database;

    @Inject
    private TableManager manager;

    @Inject
    private InternalTableDefinitions definitions;

    @Inject
    private ObjectMapper mapper;

    @POST
    public String clear(@QueryParam("preserveSchema") Boolean preserveSchema)
            throws Exception {
        boolean preserve = (preserveSchema != null) && preserveSchema;

        for (Map.Entry<String, TableDefinition> entry : manager
                .listAllTablesByName().entrySet()) {
            if (preserve) {
                database.truncateTable(manager.getTableDef(entry.getKey()));
            } else {
                manager.deleteTable(entry.getKey());
            }
        }

        database.truncateTable(definitions.getTables());
        database.truncateTable(definitions.getCounters());
        database.truncateTable(definitions.getGraph());

        Map<String, String> result = new LinkedHashMap<String, String>();
        result.put("status", "OK");

        return mapper.writeValueAsString(result);
    }
}
