package com.g414.st8.inno;

import com.g414.inno.db.ColumnAttribute;
import com.g414.inno.db.ColumnType;
import com.g414.inno.db.Database;
import com.g414.inno.db.TableBuilder;
import com.g414.inno.db.TableDef;
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
}
