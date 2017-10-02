package com.couchbase.bigfun;

public class BatchModeUpdateParameter {
    public String updateFieldName;
    public String updateFieldType;
    public String updateValuesFilePath;
    public String updateValueStart;
    public String updateValueEnd;
    public String updateValueFormat;
    public BatchModeUpdateParameter(String updateFieldName, String updateFieldType, String updateValueStart, String updateValueEnd) {
        this.updateFieldName = updateFieldName;
        this.updateFieldType = updateFieldType;
        this.updateValueStart = updateValueStart;
        this.updateValueEnd = updateValueEnd;
        this.updateValuesFilePath = "";
        this.updateValueFormat = "";
    }
    public BatchModeUpdateParameter(String updateFieldName, String updateFieldType, String updateValuesFilePath) {
        this.updateFieldName = updateFieldName;
        this.updateFieldType = updateFieldType;
        this.updateValueStart = "";
        this.updateValueEnd = "";
        this.updateValuesFilePath = updateValuesFilePath;
        this.updateValueFormat = "";
    }
    public BatchModeUpdateParameter(String updateFieldName, String updateFieldType, String updateValueFormat, String updateValueStart, String updateValueEnd) {
        this.updateFieldName = updateFieldName;
        this.updateFieldType = updateFieldType;
        this.updateValueStart = updateValueStart;
        this.updateValueEnd = updateValueEnd;
        this.updateValuesFilePath = "";
        this.updateValueFormat = updateValueFormat;
    }
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof BatchModeUpdateParameter)) {
            return false;
        }
        BatchModeUpdateParameter c = (BatchModeUpdateParameter) o;
        return updateFieldName.equals(c.updateFieldName) &&
        updateFieldType.equals(c.updateFieldType) &&
        updateValuesFilePath.equals(c.updateValuesFilePath) &&
        updateValueStart.equals(c.updateValueStart) &&
        updateValueEnd.equals(c.updateValueEnd) &&
        updateValueFormat.equals(c.updateValueFormat);
    }
}
