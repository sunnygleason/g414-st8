package com.g414.st8.inno;

import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import com.g414.inno.db.ColumnDef;
import com.g414.inno.db.Cursor;
import com.g414.inno.db.Database;
import com.g414.inno.db.IndexDef;
import com.g414.inno.db.LockMode;
import com.g414.inno.db.SearchMode;
import com.g414.inno.db.TableDef;
import com.g414.inno.db.Transaction;
import com.g414.inno.db.TransactionLevel;
import com.g414.inno.db.Tuple;
import com.g414.inno.db.TupleBuilder;
import com.g414.inno.db.TupleBuilder.Options;
import com.google.inject.Inject;

public class DataManager {
    @Inject
    private TableManager tableManager;

    @Inject
    private Database database;

    public void insertData(String tableName, Map<String, ?> data) {
        TableDef def = tableManager.getTableDef(tableName);
        Transaction txn = null;
        Cursor c = null;
        Tuple toInsert = null;
        try {
            txn = database.beginTransaction(TransactionLevel.REPEATABLE_READ);
            c = txn.openTable(def);
            c.setLockMode(LockMode.INTENTION_EXCLUSIVE);
            c.lock(LockMode.LOCK_EXCLUSIVE);

            toInsert = c.createClusteredIndexReadTuple();

            TupleBuilder tpl = new TupleBuilder(def, Options.COERCE);
            for (ColumnDef col : def.getColDefs()) {
                tpl.addValue(data.get(col.getName()));
            }

            c.insertRow(toInsert, tpl);
            toInsert.clear();
        } finally {
            if (toInsert != null) {
                toInsert.delete();
            }

            if (c != null) {
                c.close();
            }

            if (txn != null) {
                txn.commit();
            }
        }
    }

    public void updateData(String tableName, Map<String, Object> data) {
        TableDef def = tableManager.getTableDef(tableName);
        IndexDef primary = def.getPrimaryIndex();

        Transaction txn = null;
        Cursor c = null;
        Tuple toFind = null;
        Tuple toUpdate = null;
        try {
            txn = database.beginTransaction(TransactionLevel.REPEATABLE_READ);
            c = txn.openTable(def);
            c.setLockMode(LockMode.INTENTION_EXCLUSIVE);
            c.lock(LockMode.LOCK_EXCLUSIVE);

            TupleBuilder tpl = new TupleBuilder(def, Options.COERCE);
            for (ColumnDef col : primary.getColumns()) {
                tpl.addValue(data.get(col.getName()));
            }

            toFind = c.createClusteredIndexSearchTuple(tpl);
            c.find(toFind, SearchMode.GE);

            if (c.isPositioned() && c.hasNext()) {
                toUpdate = c.createClusteredIndexReadTuple();
                c.readRow(toUpdate);

                Map<String, Object> found = toUpdate.valueMap();
                for (ColumnDef col : primary.getColumns()) {
                    String colName = col.getName();
                    Object seekVal = data.get(colName);
                    Object foundVal = found.get(colName);

                    if ((seekVal == null && foundVal != null)
                            || (seekVal != null && !seekVal.equals(foundVal
                                    .toString()))) {
                        throw new WebApplicationException(Status.NOT_FOUND);
                    }
                }
            } else {
                throw new WebApplicationException(Status.NOT_FOUND);
            }

            TupleBuilder val = new TupleBuilder(def, Options.COERCE);
            for (ColumnDef col : def.getColDefs()) {
                val.addValue(data.get(col.getName()));
            }

            c.updateRow(toUpdate, val);
            toUpdate.clear();
        } finally {
            if (toUpdate != null) {
                toUpdate.delete();
            }

            if (toFind != null) {
                toFind.delete();
            }

            if (c != null) {
                c.close();
            }

            if (txn != null) {
                txn.commit();
            }
        }
    }

    public Map<String, Object> loadData(String tableName,
            Map<String, Object> data) {
        TableDef def = tableManager.getTableDef(tableName);
        IndexDef primary = def.getPrimaryIndex();

        Transaction txn = null;
        Cursor c = null;
        Tuple toFind = null;
        Tuple toReturn = null;
        try {
            txn = database.beginTransaction(TransactionLevel.REPEATABLE_READ);
            c = txn.openTable(def);
            c.setLockMode(LockMode.INTENTION_EXCLUSIVE);
            c.lock(LockMode.LOCK_EXCLUSIVE);

            TupleBuilder tpl = new TupleBuilder(def, Options.COERCE);
            for (ColumnDef col : primary.getColumns()) {
                tpl.addValue(data.get(col.getName()));
            }

            toFind = c.createClusteredIndexSearchTuple(tpl);
            c.find(toFind, SearchMode.GE);

            if (c.isPositioned() && c.hasNext()) {
                toReturn = c.createClusteredIndexReadTuple();
                c.readRow(toReturn);

                Map<String, Object> found = toReturn.valueMap();
                for (ColumnDef col : primary.getColumns()) {
                    String colName = col.getName();
                    Object seekVal = data.get(colName);
                    Object foundVal = found.get(colName);

                    if ((seekVal == null && foundVal != null)
                            || (seekVal != null && !seekVal.equals(foundVal
                                    .toString()))) {
                        throw new WebApplicationException(Status.NOT_FOUND);
                    }
                }
            } else {
                throw new WebApplicationException(Status.NOT_FOUND);
            }

            Map<String, Object> res = toReturn.valueMap();
            toReturn.clear();

            return res;
        } finally {
            if (toReturn != null) {
                toReturn.delete();
            }

            if (toFind != null) {
                toFind.delete();
            }

            if (c != null) {
                c.close();
            }

            if (txn != null) {
                txn.commit();
            }
        }
    }

    public void deleteData(String tableName, Map<String, Object> data) {
        TableDef def = tableManager.getTableDef(tableName);
        IndexDef primary = def.getPrimaryIndex();

        Transaction txn = null;
        Cursor c = null;
        Tuple toFind = null;
        Tuple toDelete = null;
        try {
            txn = database.beginTransaction(TransactionLevel.REPEATABLE_READ);
            c = txn.openTable(def);
            c.setLockMode(LockMode.INTENTION_EXCLUSIVE);
            c.lock(LockMode.LOCK_EXCLUSIVE);

            TupleBuilder tpl = new TupleBuilder(def, Options.COERCE);
            for (ColumnDef col : primary.getColumns()) {
                tpl.addValue(data.get(col.getName()));
            }

            toFind = c.createClusteredIndexSearchTuple(tpl);
            c.find(toFind, SearchMode.GE);

            if (c.isPositioned() && c.hasNext()) {
                toDelete = c.createClusteredIndexReadTuple();
                c.readRow(toDelete);

                Map<String, Object> found = toDelete.valueMap();
                for (ColumnDef col : primary.getColumns()) {
                    String colName = col.getName();
                    Object seekVal = data.get(colName);
                    Object foundVal = found.get(colName);

                    if ((seekVal == null && foundVal != null)
                            || (seekVal != null && !seekVal.equals(foundVal
                                    .toString()))) {
                        throw new WebApplicationException(Status.NOT_FOUND);
                    }
                }

                c.deleteRow();
                toDelete.clear();
            } else {
                throw new WebApplicationException(Status.NOT_FOUND);
            }
        } finally {
            if (toDelete != null) {
                toDelete.delete();
            }

            if (toFind != null) {
                toFind.delete();
            }

            if (c != null) {
                c.close();
            }

            if (txn != null) {
                txn.commit();
            }
        }
    }
}
