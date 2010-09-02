package com.g414.st8.model;

import java.util.List;

public class ColumnDefinition {
    private String name;
    private String type;
    private int length;
    private List<String> columnAttributes;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public List<String> getColumnAttributes() {
        return columnAttributes;
    }

    public void setColumnAttributes(List<String> columnAttributes) {
        this.columnAttributes = columnAttributes;
    }
}
