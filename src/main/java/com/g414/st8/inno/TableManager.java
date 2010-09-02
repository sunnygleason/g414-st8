package com.g414.st8.inno;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import org.codehaus.jackson.map.ObjectMapper;

import com.g414.inno.db.ColumnAttribute;
import com.g414.inno.db.ColumnType;
import com.g414.inno.db.Cursor;
import com.g414.inno.db.Database;
import com.g414.inno.db.LockMode;
import com.g414.inno.db.SearchMode;
import com.g414.inno.db.TableBuilder;
import com.g414.inno.db.TableDef;
import com.g414.inno.db.Transaction;
import com.g414.inno.db.TransactionLevel;
import com.g414.inno.db.Tuple;
import com.g414.inno.db.TupleBuilder;
import com.g414.st8.guice.DatabaseModule.DatabaseName;
import com.g414.st8.model.ColumnDefinition;
import com.g414.st8.model.IndexColumn;
import com.g414.st8.model.IndexDefinition;
import com.g414.st8.model.TableDefinition;
import com.google.inject.Inject;

public class TableManager {
    @Inject
    @DatabaseName
    private String databaseName;

    @Inject
    private Database database;

    @Inject
    private InternalTableDefinitions definitions;

    @Inject
    private ObjectMapper mapper;

    public Map<String, TableDefinition> listAllTables() {
        Map<String, TableDefinition> result = new LinkedHashMap<String, TableDefinition>();

        TableDef tables = definitions.getTables();
        Transaction txn = database
                .beginTransaction(TransactionLevel.REPEATABLE_READ);
        Cursor c = txn.openTable(tables);
        c.lock(LockMode.INTENTION_SHARED);
        c.setLockMode(LockMode.LOCK_SHARED);

        Tuple t = null;
        try {
            t = c.createClusteredIndexReadTuple();
            c.first();

            if (c.isPositioned()) {
                while (c.hasNext()) {
                    c.readRow(t);
                    Map<String, Object> val = t.valueMap();

                    TableDefinition value = mapper.readValue(new String(
                            (byte[]) val.get("schema")), TableDefinition.class);
                    result.put((String) val.get("name"), value);

                    c.next();
                    t.clear();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (t != null) {
                t.delete();
            }

            c.close();
            txn.commit();
        }

        try {
            return result;
        } catch (Exception ignored) {
            ignored.printStackTrace();
            throw new RuntimeException(ignored);
        }
    }

    public Map<String, TableDefinition> listAllTablesByName() {
        Map<String, TableDefinition> result = new LinkedHashMap<String, TableDefinition>();

        TableDef tables = definitions.getTables();
        Transaction txn = database
                .beginTransaction(TransactionLevel.REPEATABLE_READ);
        Cursor tbl = txn.openTable(tables);
        tbl.lock(LockMode.INTENTION_SHARED);
        tbl.setLockMode(LockMode.LOCK_SHARED);

        Cursor c = tbl.openIndex("byName");
        c.setClusterAccess();

        Tuple t = null;
        try {
            t = tbl.createClusteredIndexReadTuple();
            c.first();

            if (c.isPositioned()) {
                while (c.hasNext()) {
                    c.readRow(t);
                    Map<String, Object> val = t.valueMap();

                    TableDefinition value = mapper.readValue(new String(
                            (byte[]) val.get("schema")), TableDefinition.class);
                    result.put((String) val.get("name"), value);

                    c.next();
                    t.clear();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (t != null) {
                t.delete();
            }

            c.close();
            tbl.close();
            txn.commit();
        }

        try {
            return result;
        } catch (Exception ignored) {
            ignored.printStackTrace();
            throw new RuntimeException(ignored);
        }
    }

    public TableDefinition getTable(String name, boolean doThrow) {
        TableDefinition result = null;

        TableDef tables = definitions.getTables();

        Transaction txn = database
                .beginTransaction(TransactionLevel.REPEATABLE_READ);
        Cursor tbl = txn.openTable(tables);
        tbl.lock(LockMode.INTENTION_SHARED);
        tbl.setLockMode(LockMode.LOCK_SHARED);

        Cursor c = tbl.openIndex("byName");
        c.setClusterAccess();
        c.lock(LockMode.INTENTION_SHARED);
        c.setLockMode(LockMode.LOCK_SHARED);

        Tuple t = null;
        Tuple u = null;

        try {
            TupleBuilder builder = new TupleBuilder(tables).addValue(name);
            t = c.createSecondaryIndexSearchTuple(builder);

            c.find(t, SearchMode.GE);

            if (c.isPositioned() && c.hasNext()) {
                u = tbl.createClusteredIndexReadTuple();
                c.readRow(u);

                Map<String, Object> val = u.valueMap();

                if (val.get("name").equals(name)) {
                    TableDefinition value = mapper.readValue(new String(
                            (byte[]) val.get("schema")), TableDefinition.class);
                    result = value;
                }

                u.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (t != null) {
                t.delete();
            }
            if (u != null) {
                u.delete();
            }

            c.close();
            tbl.close();
            txn.commit();
        }

        if (result == null && doThrow) {
            throw new WebApplicationException(HttpServletResponse.SC_NOT_FOUND);
        }

        return result;
    }

    public void createTable(String name, TableDefinition tableDefinition) {
        // FIXME : make this actually transactional
        if (this.getTable(name, false) != null) {
            throw new WebApplicationException(Status.CONFLICT);
        }

        TableDef tables = definitions.getTables();
        Transaction txn = database
                .beginTransaction(TransactionLevel.REPEATABLE_READ);

        Cursor c = txn.openTable(tables);
        c.setClusterAccess();
        c.lock(LockMode.INTENTION_EXCLUSIVE);

        Tuple t = null;
        TupleBuilder toInsert = new TupleBuilder(tables);
        Tuple i = null;

        int prev = 0;

        try {
            t = c.createClusteredIndexReadTuple();

            c.last();

            if (c.isPositioned() && c.hasNext()) {
                c.readRow(t);
                prev = ((Number) t.getInteger(0)).intValue();
                t.clear();
            }

            c.setLockMode(LockMode.LOCK_EXCLUSIVE);

            toInsert.addValue(prev + 1);
            toInsert.addValue(name);
            toInsert.addValue(mapper.writeValueAsString(tableDefinition)
                    .getBytes());

            i = c.createClusteredIndexReadTuple();

            c.insertRow(i, toInsert);
            i.clear();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (t != null) {
                t.delete();
            }

            if (i != null) {
                i.delete();
            }

            c.close();
            txn.commit();
        }

        TableDef def = createFromTableDefinition(name, tableDefinition);
        database.createTable(def);
    }

    public void truncateTable(String name) {
        TableDefinition definition = this.getTable(name, true);
        TableDef tableDef = createFromTableDefinition(name, definition);
        if (database.tableExists(tableDef)) {
            database.truncateTable(tableDef);
        }
    }

    public void deleteTable(String name) {
        TableDefinition definition = this.getTable(name, true);
        TableDef tableDef = createFromTableDefinition(name, definition);
        try {
            if (database.tableExists(tableDef)) {
                database.dropTable(tableDef);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            this.deleteTableRow(name);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void deleteTableRow(String name) {
        TableDef tables = definitions.getTables();
        Transaction txn = database
                .beginTransaction(TransactionLevel.REPEATABLE_READ);
        Cursor tbl = txn.openTable(tables);
        tbl.lock(LockMode.INTENTION_EXCLUSIVE);
        tbl.setLockMode(LockMode.LOCK_EXCLUSIVE);

        Cursor c = tbl.openIndex("byName");
        c.setClusterAccess();
        c.lock(LockMode.INTENTION_EXCLUSIVE);
        c.setLockMode(LockMode.LOCK_EXCLUSIVE);

        Tuple t = null;
        Tuple u = null;
        boolean found = false;

        try {
            TupleBuilder builder = new TupleBuilder(tables).addValue(name);
            t = c.createSecondaryIndexSearchTuple(builder);
            c.find(t, SearchMode.GE);

            if (c.isPositioned() && c.hasNext()) {
                u = tbl.createClusteredIndexReadTuple();
                c.readRow(u);

                Map<String, Object> val = u.valueMap();

                if (val.get("name").equals(name)) {
                    c.deleteRow();
                    found = true;
                }

                u.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (t != null) {
                t.delete();
            }
            if (u != null) {
                u.delete();
            }

            c.close();
            tbl.close();
            txn.commit();
        }

        if (!found) {
            throw new WebApplicationException(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    public TableDef getTableDef(String name) {
        TableDefinition inDef = this.getTable(name, true);
        return createFromTableDefinition(name, inDef);
    }

    private TableDef createFromTableDefinition(String name,
            TableDefinition inDef) {
        TableBuilder builder = new TableBuilder(databaseName + "/" + name);
        if (inDef.getColumns() != null) {
            for (ColumnDefinition c : inDef.getColumns()) {
                ColumnAttribute[] attrs = new ColumnAttribute[0];
                if (c.getColumnAttributes() != null) {
                    attrs = new ColumnAttribute[c.getColumnAttributes().size()];
                    int i = 0;
                    for (String a : c.getColumnAttributes()) {
                        attrs[i++] = ColumnAttribute.valueOf(a);
                    }
                }
                builder.addColumn(c.getName(), ColumnType.valueOf(c.getType()),
                        c.getLength(), attrs);
            }
        }

        if (inDef.getIndexes() != null) {
            for (IndexDefinition c : inDef.getIndexes()) {
                for (IndexColumn n : c.getIndexColumns()) {
                    builder.addIndex(c.getName(), n.getName(),
                            n.getPrefixLen(), c.isClustered(), c.isUnique());
                }
            }
        }

        return builder.build();
    }
}
