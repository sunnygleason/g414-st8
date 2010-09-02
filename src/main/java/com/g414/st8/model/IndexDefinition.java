package com.g414.st8.model;

import java.util.List;

public class IndexDefinition {
    private String name;
    private boolean clustered;
    private boolean unique;
    private List<IndexColumn> indexColumns;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isClustered() {
        return clustered;
    }

    public void setClustered(boolean clustered) {
        this.clustered = clustered;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public List<IndexColumn> getIndexColumns() {
        return indexColumns;
    }

    public void setIndexColumns(List<IndexColumn> indexColumns) {
        this.indexColumns = indexColumns;
    }
}
