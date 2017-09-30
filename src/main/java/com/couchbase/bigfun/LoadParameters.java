package com.couchbase.bigfun;

import java.util.ArrayList;
import java.util.Arrays;

public class LoadParameters {
    public ArrayList<PartitionLoadParameter> partitionLoadParameters = new ArrayList<PartitionLoadParameter>();
    public LoadParameters(){
        this(null);
    }
    public LoadParameters(ArrayList<PartitionLoadParameter> partitionLoadParameters){
        if (partitionLoadParameters != null) {
            this.partitionLoadParameters = partitionLoadParameters;
        }
    }
    @Override
    public boolean equals(Object o) {
       if (o == this) {
            return true;
        }
        if (!(o instanceof LoadParameters)) {
            return false;
        }
        LoadParameters c = (LoadParameters) o;
        return partitionLoadParameters.equals(c.partitionLoadParameters);
    }
}
