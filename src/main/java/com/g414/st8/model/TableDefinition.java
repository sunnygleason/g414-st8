package com.g414.st8.model;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class TableDefinition {
    private String tableType;
    private int pageSize;
    private List<ColumnDefinition> columns;
    private List<IndexDefinition> indexes;

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public List<ColumnDefinition> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnDefinition> columns) {
        this.columns = columns;
    }

    public List<IndexDefinition> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<IndexDefinition> indexes) {
        this.indexes = indexes;
    }
}
