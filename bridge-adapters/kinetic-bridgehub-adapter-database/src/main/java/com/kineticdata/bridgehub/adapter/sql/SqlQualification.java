package com.kineticdata.bridgehub.adapter.sql;

import java.util.*;

public class SqlQualification {
    private String parameterizedString;
    private List<SqlQualificationParameter> parameters;

    public SqlQualification(String parameterizedString, List<SqlQualificationParameter> parameters) {
        this.parameterizedString = parameterizedString;
        this.parameters = parameters;
    }

    public String getParameterizedString() {
        return parameterizedString;
    }

    public void setParameterizedString(String parameterizedString) {
        this.parameterizedString = parameterizedString;
    }

    public List<SqlQualificationParameter> getParameters() {
        return parameters;
    }

    public void setParameters(List<SqlQualificationParameter> parameters) {
        this.parameters = parameters;
    }
}
