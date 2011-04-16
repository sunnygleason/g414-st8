package com.g414.st8.haildb;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import org.codehaus.jackson.map.ObjectMapper;

import com.g414.haildb.ColumnAttribute;
import com.g414.haildb.ColumnType;
import com.g414.haildb.Cursor.CursorDirection;
import com.g414.haildb.Cursor.SearchMode;
import com.g414.haildb.Database;
import com.g414.haildb.TableBuilder;
import com.g414.haildb.TableDef;
import com.g414.haildb.Transaction;
import com.g414.haildb.Transaction.TransactionLevel;
import com.g414.haildb.tpl.DatabaseTemplate;
import com.g414.haildb.tpl.DatabaseTemplate.TransactionCallback;
import com.g414.haildb.tpl.Functional;
import com.g414.haildb.tpl.Functional.Filter;
import com.g414.haildb.tpl.Functional.Mapping;
import com.g414.haildb.tpl.Functional.Mutation;
import com.g414.haildb.tpl.Functional.MutationType;
import com.g414.haildb.tpl.Functional.Reduction;
import com.g414.haildb.tpl.Functional.Target;
import com.g414.haildb.tpl.Functional.Traversal;
import com.g414.haildb.tpl.Functional.TraversalSpec;
import com.g414.st8.guice.DatabaseModule.DatabaseName;
import com.g414.st8.model.ColumnDefinition;
import com.g414.st8.model.IndexColumn;
import com.g414.st8.model.IndexDefinition;
import com.g414.st8.model.TableDefinition;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

public class TableManager {
    @Inject
    @DatabaseName
    private String databaseName;

    @Inject
    private Database database;

    @Inject
    private DatabaseTemplate dbt;

    @Inject
    private InternalTableDefinitions definitions;

    @Inject
    private ObjectMapper mapper;

    private Map<String, TableDef> defCache = new ConcurrentHashMap<String, TableDef>();

    public Map<String, TableDefinition> listAllTables() throws Exception {
        return getAllTables(new Target(definitions.getTables()));
    }

    public Map<String, TableDefinition> listAllTablesByName() throws Exception {
        return getAllTables(new Target(definitions.getTables(), "byName"));
    }

    public TableDefinition getTable(final String name, boolean doThrow)
            throws Exception {
        final Filter primaryFilter = getTableNamePrimaryFilter(name);

        TableDefinition result = dbt.inTransaction(
                TransactionLevel.READ_COMMITTED,
                new TransactionCallback<TableDefinition>() {
                    @Override
                    public TableDefinition inTransaction(Transaction txn) {
                        Traversal<TableDefinition> iter = Functional.map(
                                txn,
                                new TraversalSpec(new Target(definitions
                                        .getTables(), "byName"), ImmutableMap
                                        .<String, Object> of("name", name),
                                        primaryFilter, null),
                                new Mapping<TableDefinition>() {
                                    @Override
                                    public TableDefinition map(
                                            Map<String, Object> row) {
                                        try {
                                            return mapper.readValue(
                                                    new String((byte[]) row
                                                            .get("schema")),
                                                    TableDefinition.class);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                });

                        if (iter.hasNext()) {
                            return iter.next();
                        }

                        return null;
                    }
                });

        if (result == null && doThrow) {
            throw new WebApplicationException(HttpServletResponse.SC_NOT_FOUND);
        }

        return result;
    }

    public void createTable(final String name,
            final TableDefinition tableDefinition) throws Exception {
        if (this.getTable(name, false) != null) {
            throw new WebApplicationException(Status.CONFLICT);
        }

        final TableDef def = createFromTableDefinition(name, tableDefinition);
        final TraversalSpec spec = new TraversalSpec(new Target(
                definitions.getTables()), CursorDirection.DESC, SearchMode.LE,
                null, getAlwaysTruePrimaryFilter(), null);

        dbt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    public Void inTransaction(Transaction txn) {

                        database.createTable(def);

                        Traversal<Long> iter = Functional.map(txn, spec,
                                new Mapping<Long>() {
                                    @Override
                                    public Long map(Map<String, Object> row) {
                                        return ((Number) row.get("id"))
                                                .longValue();
                                    }
                                });

                        Long prev;
                        try {
                            prev = iter.hasNext() ? iter.next() : 0L;

                            final Map<String, Object> toInsert = ImmutableMap.<String, Object> of(
                                    "id", Long.valueOf(prev + 1), "name", name,
                                    "schema",
                                    mapper.writeValueAsString(tableDefinition)
                                            .getBytes());

                            dbt.insert(txn, definitions.getTables(), toInsert);
                        } catch (Exception e) {
                            if (database.tableExists(def)) {
                                database.dropTable(def);
                            }

                            throw new RuntimeException(e);
                        } finally {
                            iter.close();
                        }

                        return null;
                    }
                });
    }

    public synchronized void truncateTable(String name) throws Exception {
        TableDefinition definition = this.getTable(name, true);
        TableDef tableDef = createFromTableDefinition(name, definition);

        if (database.tableExists(tableDef)) {
            database.truncateTable(tableDef);
        }
    }

    public synchronized void deleteTable(String name) throws Exception {
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

        this.defCache.remove(name);
    }

    public TableDef getTableDef(String name) throws Exception {
        if (!defCache.containsKey(name)) {
            TableDefinition inDef = this.getTable(name, true);

            defCache.put(name, createFromTableDefinition(name, inDef));
        }

        return defCache.get(name);
    }

    private void deleteTableRow(final String name) throws Exception {
        final Filter primaryFilter = getTableNamePrimaryFilter(name);

        final AtomicBoolean deleted = new AtomicBoolean();

        final Mapping<Mutation> mutation = new Mapping<Mutation>() {
            @Override
            public Mutation map(Map<String, Object> row) {
                deleted.set(true);

                return new Mutation(MutationType.DELETE, row);
            }
        };

        dbt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        Functional.apply(
                                txn,
                                dbt,
                                new TraversalSpec(new Target(definitions
                                        .getTables(), "byName"), ImmutableMap
                                        .<String, Object> of("name", name),
                                        primaryFilter, null), mutation)
                                .traverseAll();

                        return null;
                    }
                });

        if (!deleted.get()) {
            throw new WebApplicationException(HttpServletResponse.SC_NOT_FOUND);
        }
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

    private Map<String, TableDefinition> getAllTables(final Target target)
            throws Exception {
        final Filter primaryFilter = getAlwaysTruePrimaryFilter();

        final Reduction<Map<String, TableDefinition>> reduction = new Reduction<Map<String, TableDefinition>>() {
            public Map<String, TableDefinition> reduce(Map<String, Object> row,
                    Map<String, TableDefinition> initial) {
                try {
                    TableDefinition value = mapper.readValue(new String(
                            (byte[]) row.get("schema")), TableDefinition.class);

                    initial.put((String) row.get("name"), value);

                    return initial;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        return dbt.inTransaction(TransactionLevel.READ_COMMITTED,
                new TransactionCallback<Map<String, TableDefinition>>() {
                    public Map<String, TableDefinition> inTransaction(
                            Transaction txn) {
                        return Functional.reduce(txn, new TraversalSpec(target,
                                null, primaryFilter, null), reduction,
                                new LinkedHashMap<String, TableDefinition>());
                    }
                });
    }

    private Filter getAlwaysTruePrimaryFilter() {
        return new Filter() {
            public Boolean map(Map<String, Object> row) {
                return true;
            }
        };
    }

    private Filter getTableNamePrimaryFilter(final String name) {
        final Filter primaryFilter = new Filter() {
            @Override
            public Boolean map(Map<String, Object> row) {
                return row.get("name").equals(name);
            }
        };
        return primaryFilter;
    }
}
