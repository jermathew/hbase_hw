package it.uniroma3.dia.sii.hbase;

import java.util.ArrayList;
import java.util.List;

import it.uniroma3.dia.sii.hbase.model.RowBean;
import it.uniroma3.dia.sii.hbase.utils.HBaseWrapper;
import it.uniroma3.dia.sii.hbase.utils.Tuple;

public class HBaseConnector {

    public static void main(String[] args) throws Exception {
        HBaseWrapper wrapper= new HBaseWrapper();
        List<String> family= new ArrayList<>();
        family.add("informatica");
        family.add("automazione");
        String tableName="dia";
        wrapper.createTable(tableName,family);
        wrapper.addRecord(tableName,"1","informatica","IA","user1");
        wrapper.addRecord(tableName,"2","informatica","IA","user2");
        wrapper.addRecord(tableName,"3","informatica","BD","user3");
        wrapper.addRecord(tableName,"4","informatica","BD","user4");
        wrapper.addRecord(tableName,"5","informatica","TLC","user5");
        wrapper.addRecord(tableName,"6","automazione","ASE","user6");
        wrapper.addRecord(tableName,"7","automazione","ASE","user7");
        wrapper.addRecord(tableName,"8","automazione","APS","user8");
        wrapper.addRecord(tableName,"9","automazione","FDA","user9");
        wrapper.addRecord(tableName,"10","automazione","RO","user10");

        for (RowBean row:wrapper.scanByColumnFamily(tableName,"informatica"))
        {
            System.out.println(row.toString());
        }

        for (RowBean row:wrapper.scanByColumnQualifier(tableName,"informatica","BD"))
        {
            System.out.println(row.toString());
        }

        wrapper.deleteTable("dia");







    }
}
