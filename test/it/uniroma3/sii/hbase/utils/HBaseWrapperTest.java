package it.uniroma3.sii.hbase.utils;

import it.uniroma3.dia.sii.hbase.model.RowBean;
import it.uniroma3.dia.sii.hbase.utils.HBaseWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class HBaseWrapperTest {
    Configuration config;

    HTable table;


    HBaseWrapper wrapper= new HBaseWrapper();

    @Before
    public void setUp() throws IOException {
        config=HBaseConfiguration.create();
        {
            try {
                table = new HTable(config, "test");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        HBaseWrapper wrapper= new HBaseWrapper();
        List<String> listOfFamilies=new ArrayList<>();
        listOfFamilies.add("family1");
        listOfFamilies.add("family2");
        wrapper.createTable("test",listOfFamilies);

    }



    @Test
    public void testPutAndRetrieveRecord() throws Exception {
        wrapper.addRecord("test","row1","family1","qualifier1","value1");
        RowBean rb=wrapper.getOneRecord("test","row1");
        assertEquals("value1",rb.getValue("family1","qualifier1"));
    }

    @Test
    public void deleteRecord() throws Exception {
        wrapper.addRecord("test","row2","family1","qualifier2","value2");
        RowBean beforeDelete=wrapper.getOneRecord("test","row2");
        assertEquals("value2",beforeDelete.getValue("family1","qualifier2"));
        wrapper.delRecord("test","row2");
        RowBean rb=wrapper.getOneRecord("test","row2");
        assertNull(rb.getValue("family1","qualifier2"));
    }

    @Test
    public void testGetAllRowByFamilyName() throws Exception {
        wrapper.addRecord("test","row3","family2","qualifier1","value3");
        wrapper.addRecord("test","row4","family2","qualifier2","value4");
        List<RowBean> queryResult=wrapper.scanByColumnFamily("test","family2");
        assertEquals(2,queryResult.size());
        assertEquals("value3",queryResult.get(0).getValue("family2","qualifier1"));
        assertEquals("value4",queryResult.get(1).getValue("family2","qualifier2"));
    }

    @Test
    public void testGetAllRowByQualifierName() throws Exception {
        wrapper.addRecord("test","row5","family2","qualifier3","value5");
        wrapper.addRecord("test","row6","family2","qualifier3","value6");
        List<RowBean> queryResult=wrapper.scanByColumnQualifier("test","family2","qualifier3");
        assertEquals(2,queryResult.size());
        assertEquals("value5",queryResult.get(0).getValue("family2","qualifier3"));
        assertEquals("value6",queryResult.get(1).getValue("family2","qualifier3"));
    }

    @After
    public void tearDown() throws IOException {
        wrapper.deleteTable("test");
    }

}