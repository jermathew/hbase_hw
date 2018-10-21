package it.uniroma3.dia.sii.hbase.model;

import it.uniroma3.dia.sii.hbase.utils.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RowBean {
    private Map<Tuple<String,String>,String> rowData;
    private String rowId;

    public RowBean(String rowId){
        this.rowId=rowId;
        this.rowData=new HashMap<>();
    }


    public void setRowId(String rowId) {
        this.rowId = rowId;
    }

    public Map<Tuple<String, String>, String> getRowData() {
        return rowData;
    }

    public void setRowData(Map<Tuple<String, String>, String> rowData) {
        this.rowData = rowData;
    }

    public String getRowId() {
        return rowId;
    }

    public void addColumnValue(String family, String qualifier, String value){
        Tuple<String,String> column=new Tuple<>(family,qualifier);
        this.rowData.put(column,value);
    }
    public String getValue(String family,String qualifier){
        Tuple<String,String> column= new Tuple<>(family,qualifier);
        return this.rowData.get(column);
    }

    public Set<Tuple<String,String>> getColumnKeys(){
        return this.rowData.keySet();
    }

    @Override
    public String toString(){
        String result="RowId: " + this.rowId + " ";
        Set<Tuple<String,String>> setOfColumns=this.rowData.keySet();
        for(Tuple<String,String> columnKey: setOfColumns){
            String value=this.rowData.get(columnKey);
            result+="| " + columnKey.getA() + ":" + columnKey.getB() + "= " + value + " ";
        }
        return result;
    }
}