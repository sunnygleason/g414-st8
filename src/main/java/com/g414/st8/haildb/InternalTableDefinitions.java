package com.g414.st8.haildb;

import com.g414.haildb.ColumnAttribute;
import com.g414.haildb.ColumnType;
import com.g414.haildb.Database;
import com.g414.haildb.TableBuilder;
import com.g414.haildb.TableDef;
import com.g414.st8.guice.DatabaseModule.DatabaseName;
import com.google.inject.Inject;

public class InternalTableDefinitions {
    @Inject
    @DatabaseName
    private String databaseName;

    @Inject
    private Database database;

    public TableDef getTables() {
        TableBuilder builder = new TableBuilder(databaseName + "/" + "_TABLES");
        builder.addColumn("id", ColumnType.INT, 4, ColumnAttribute.UNSIGNED);
        builder.addColumn("name", ColumnType.VARCHAR, 80);
        builder.addColumn("schema", ColumnType.BLOB, 0);

        builder.addIndex("byId", "id", 0, true, true);
        builder.addIndex("byName", "name", 0, false, true);

        TableDef def = builder.build();

        if (!database.tableExists(def)) {
            database.createTable(def);
        }

        return def;
    }

    public TableDef getCounters() {
        TableBuilder builder = new TableBuilder(databaseName + "/"
                + "_COUNTERS");
        builder.addColumn("id", ColumnType.INT, 8, ColumnAttribute.UNSIGNED);
        builder.addColumn("key_hash", ColumnType.INT, 8,
                ColumnAttribute.UNSIGNED);
        builder.addColumn("key", ColumnType.VARCHAR, 80);
        builder.addColumn("count", ColumnType.INT, 8, ColumnAttribute.UNSIGNED);

        builder.addIndex("P", "id", 0, true, true);

        builder.addIndex("K", "key_hash", 0, false, true);
        builder.addIndex("K", "key", 80, false, true);

        TableDef def = builder.build();

        if (!database.tableExists(def)) {
            database.createTable(def);
        }

        return def;
    }

    public TableDef getGraph() {
        TableBuilder builder = new TableBuilder(databaseName + "/" + "_GRAPH");

        builder.addColumn("a", ColumnType.INT, 8);
        builder.addColumn("b", ColumnType.INT, 8);
        builder.addColumn("c", ColumnType.INT, 8);

        builder.addIndex("P", "a", 0, true, true);
        builder.addIndex("P", "b", 0, true, true);
        builder.addIndex("P", "c", 0, true, true);

        builder.addIndex("R", "c", 0, false, false);
        builder.addIndex("R", "b", 0, false, false);
        builder.addIndex("R", "a", 0, false, false);

        TableDef def = builder.build();

        if (!database.tableExists(def)) {
            database.createTable(def);
        }

        return def;
    }
}
