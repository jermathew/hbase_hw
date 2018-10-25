package it.uniroma3.dia.sii.hbase.utils;

import it.uniroma3.dia.sii.hbase.model.RowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseWrapper {

    private Configuration config;

    public HBaseWrapper(){
        this.config= HBaseConfiguration.create();
    }

    public void addRecord(String tableName,String rowKey, String family,String qualifier,String value) throws Exception{
        // instantiate HTable class
        HTable hTable = new HTable(this.config, tableName);
        // instantiate Put class
        Put p = new Put(Bytes.toBytes(rowKey));
        // add values using add() method
        p.add(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));
        // save the put Instance to the HTable.
        hTable.put(p);
        System.out.println("data inserted successfully");
    }

    public void delRecord(String tableName,String rowKey) throws IOException{
        HTable table = new HTable(this.config, tableName);

        // instantiate Delete class
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // delete the data
        table.delete(delete);

        table.close();
        System.out.println("data deleted successfully.....");
    }

    public RowBean getOneRecord(String tableName, String rowKey) throws IOException{
        HTable table = new HTable(this.config, tableName);

        // instantiate Get class
        Get g = new Get(Bytes.toBytes(rowKey));
        // get the Result object
        Result result = table.get(g);
        table.close();
        RowBean rb = parseResult(result,rowKey);
        return rb;
    }

    private RowBean parseResult(Result result, String rowKey){
        RowBean rb = new RowBean(rowKey);
        for(KeyValue kv : result.raw()){
            rb.addColumnValue((new String(kv.getFamily())), (new String(kv.getQualifier())),
                    (new String(kv.getValue())));
        }
        return rb;
    }

    public List<RowBean> scanByColumnFamily(String tableName,String familyName) throws IOException {
        HTable table = new HTable(this.config, tableName);

        // instantiate the Scan class
        Scan scan = new Scan();

        // scan the columns
        scan.addFamily(Bytes.toBytes(familyName));

        // get the ResultScanner
        ResultScanner scanner = table.getScanner(scan);
        List<RowBean> listOfRows= new ArrayList<>();
        for (Result result = scanner.next(); result != null; result=scanner.next()){
            System.out.println("Found row : " + result);
            RowBean row= parseResult(result,new String(result.getRow()));
            listOfRows.add(row);
        }

        scanner.close();
        return listOfRows;
    }

    public List<RowBean> scanByColumnQualifier(String tableName,String familyName, String qualifier) throws IOException {
        HTable table = new HTable(this.config, tableName);

        // instantiate the Scan class
        Scan scan = new Scan();

        // scan the columns
        scan.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(qualifier));

        // get the ResultScanner
        ResultScanner scanner = table.getScanner(scan);
        List<RowBean> listOfRows= new ArrayList<>();
        for (Result result = scanner.next(); result != null; result=scanner.next()){
            System.out.println("Found row : " + result);
            RowBean row= parseResult(result,new String(result.getRow()));
            listOfRows.add(row);
        }

        scanner.close();
        return listOfRows;
    }

    public void createTable(String tableName, List<String> listOfFamilies) throws IOException {
        HBaseConfiguration hc = new HBaseConfiguration(new Configuration());
        HTableDescriptor ht = new HTableDescriptor(tableName);

        for (String family: listOfFamilies)
        {
            ht.addFamily( new HColumnDescriptor(family));
        }


        System.out.println( "connecting" );
        HBaseAdmin hba = new HBaseAdmin( hc );

        System.out.println( "Creating Table" );
        hba.createTable( ht );
        System.out.println("Done......");
    }

    public void deleteTable(String tableName) throws IOException {

        // Instantiating HBaseAdmin class
        HBaseAdmin admin = new HBaseAdmin(this.config);

        admin.disableTable(tableName);

        admin.deleteTable(tableName);
        System.out.println("Table deleted");
    }
    
}
