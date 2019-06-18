package com.kineticdata.bridgehub.adapter.sql;

public class SqlQualificationParameter {
    private Integer index;
    private String name;

    public SqlQualificationParameter(Integer index, String name) {
        this.index = index;
        this.name = name;
    }

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
