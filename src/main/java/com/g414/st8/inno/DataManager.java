package com.g414.st8.inno;

import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import com.g414.inno.db.TableDef;
import com.g414.inno.db.Transaction;
import com.g414.inno.db.TransactionLevel;
import com.g414.inno.db.tpl.DatabaseTemplate;
import com.g414.inno.db.tpl.TransactionCallback;
import com.google.inject.Inject;

public class DataManager {
    @Inject
    private TableManager tableManager;

    @Inject
    private DatabaseTemplate template;

    public void insertData(String tableName, final Map<String, Object> data)
            throws Exception {
        final TableDef def = tableManager.getTableDef(tableName);
        template.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    public Void inTransaction(Transaction txn) {
                        template.insert(txn, def, data);
                        return null;
                    };
                });
    }

    public void updateData(String tableName, final Map<String, Object> data)
            throws Exception {
        final TableDef def = tableManager.getTableDef(tableName);
        template.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    public Void inTransaction(Transaction txn) {
                        if (!template.update(txn, def, data)) {
                            throw new WebApplicationException(Status.NOT_FOUND);
                        }
                        return null;
                    };
                });
    }

    public Map<String, Object> loadData(String tableName,
            final Map<String, Object> data) throws Exception {
        final TableDef def = tableManager.getTableDef(tableName);
        return template.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Map<String, Object>>() {
                    public Map<String, Object> inTransaction(Transaction txn) {
                        Map<String, Object> found = template.load(txn, def,
                                data);
                        if (found != null) {
                            return found;
                        } else {
                            throw new WebApplicationException(Status.NOT_FOUND);
                        }
                    };
                });
    }

    public void deleteData(String tableName, final Map<String, Object> data)
            throws Exception {
        final TableDef def = tableManager.getTableDef(tableName);
        template.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    public Void inTransaction(Transaction txn) {
                        if (!template.update(txn, def, data)) {
                            throw new WebApplicationException(Status.NOT_FOUND);
                        }
                        return null;
                    };
                });
    }
}
