package com.couchbase.bigfun;

import java.util.ArrayList;

public class MixModeLoadParameters {
    public ArrayList<MixModeLoadParameter> loadParameters = new ArrayList<MixModeLoadParameter>();
    public MixModeLoadParameters(){
        this(null);
    }
    public MixModeLoadParameters(ArrayList<MixModeLoadParameter> loadParameters){
        if (loadParameters != null) {
            this.loadParameters = loadParameters;
        }
    }
    @Override
    public boolean equals(Object o) {
       if (o == this) {
            return true;
        }
        if (!(o instanceof MixModeLoadParameters)) {
            return false;
        }
        MixModeLoadParameters c = (MixModeLoadParameters) o;
        return loadParameters.equals(c.loadParameters);
    }
}
