package com.couchbase.bigfun;

import java.util.ArrayList;

public class BatchModeLoadParameters {
    public ArrayList<BatchModeLoadParameter> loadParameters = new ArrayList<BatchModeLoadParameter>();
    public BatchModeLoadParameters(){
        this(null);
    }
    public BatchModeLoadParameters(ArrayList<BatchModeLoadParameter> loadParameters){
        if (loadParameters != null) {
            this.loadParameters = loadParameters;
        }
    }
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof BatchModeLoadParameters)) {
            return false;
        }
        BatchModeLoadParameters c = (BatchModeLoadParameters) o;
        return loadParameters.equals(c.loadParameters);
    }
}
