package BatchDataPacket.SubClassDataType.DBStore;

public class DBCondition{
    String colName;
    String dataType;
    String value;

    public DBCondition(String colName, String dataType, String value) {
        this.colName = colName;
        this.dataType = dataType;
        this.value = value;
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
