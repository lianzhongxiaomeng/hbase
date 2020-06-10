package com.dudu.hbase.demo;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HbaseClientDML {
	Connection conn = null;
	@Before
	public void getConnection() throws Exception {
		// ����һ�����Ӷ���
		Configuration conf = HBaseConfiguration.create();// ���Զ�����hbase��site.xml
		conf.set("hbase.zookeeper.quorum", "hdp-01:2181,hdp-02:2181,hdp-03:2181");
		conn = ConnectionFactory.createConnection(conf);
	}

	/**
	 * DML
	 * @throws Exception 
	 */
	@Test
	public void testPut() throws Exception {
		//��ȡһ������ָ�����table���󣬽���DML����
		Table table = conn.getTable(TableName.valueOf("user_info"));
		
		//����Ҫ���� һ��put���ͣ�һ��put����ֻ�ܶ�Ӧһ��rowkey���Ķ���
		Put put = new Put(Bytes.toBytes(001));
		
		put.addColumn("base_info".getBytes(), "username".getBytes(), Bytes.toBytes("aqua"));
		put.addColumn("base_info".getBytes(), "age".getBytes(), Bytes.toBytes("5"));
		put.addColumn("extra_info".getBytes(), "addr".getBytes(), Bytes.toBytes("sea"));
		
		Put put2 = new Put(Bytes.toBytes("002"));
		
		put2.addColumn("base_info".getBytes(), "username".getBytes(), Bytes.toBytes("mea"));
		put2.addColumn("base_info".getBytes(), "age".getBytes(), Bytes.toBytes("38"));
		put2.addColumn("extra_info".getBytes(), "addr".getBytes(), Bytes.toBytes("bilibili"));
		

		ArrayList<Put> list = new ArrayList<Put>();
		list.add(put);
		list.add(put2);
		
		table.put(list);
		
		table.close();
		conn.close();
	}
	
	@Test
	public void testDelete() throws Exception {
		
		Table table = conn.getTable(TableName.valueOf("user_info"));
		
		Delete delete = new Delete(Bytes.toBytes(001));
		
		Delete delete2 = new Delete(Bytes.toBytes("002"));
		delete2.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("addr"));
		
		ArrayList<Delete> del = new ArrayList<Delete>();
		del.add(delete);
		del.add(delete2);
		
		table.delete(del);
		
		table.close();
		conn.close();
		
	}
	@Test
	public void testGet() throws Exception {
		Table table = conn.getTable(TableName.valueOf("user_info"));
		
		Get get = new Get("002".getBytes());
		
		Result result = table.get(get);
		
		byte[] value = result.getValue("base_info".getBytes(), "username".getBytes());
		System.out.println(new String(value));
		
		System.out.println("_____________________________");
		
		CellScanner cellScanner = result.cellScanner();
		while(cellScanner.advance()) {
			Cell cell = cellScanner.current();
			byte[] rowArray = cell.getRowArray();//��kv�����м���ֽ�����
			byte[] familyArray = cell.getFamilyArray();//���������ֽ�����
			byte[] qualifierArray = cell.getQualifierArray();//�������ֽ�����
			byte[] valueArray = cell.getValueArray();//value���ֽ�����
			
			System.out.println("�м�"+new String(rowArray,cell.getRowOffset(),cell.getRowLength()));
			System.out.println("������"+new String(familyArray,cell.getFamilyOffset(),cell.getFamilyLength()));
			System.out.println("����"+new String(qualifierArray,cell.getQualifierOffset(),cell.getQualifierLength()));
			System.out.println("value"+new String(valueArray,cell.getValueOffset(),cell.getValueLength()));
		}
		
		table.close();
		conn.close();
		
	}
	@Test
	public void testScan() throws Exception {
	
		Table table = conn.getTable(TableName.valueOf("user_info"));
		
		Scan scan = new Scan("002".getBytes(),"002".getBytes());
		
		ResultScanner scanner = table.getScanner(scan);
		
		Iterator<Result> iterator = scanner.iterator();
		
		while(iterator.hasNext()) {
			Result next = iterator.next();
			CellScanner cellScanner = next.cellScanner();
			while(cellScanner.advance()) {
				Cell cell = cellScanner.current();
				
				byte[] rowArray = cell.getRowArray();//��kv�����м���ֽ�����
				byte[] familyArray = cell.getFamilyArray();//���������ֽ�����
				byte[] qualifierArray = cell.getQualifierArray();//�������ֽ�����
				byte[] valueArray = cell.getValueArray();//value���ֽ�����
				
				System.out.println("�м�"+new String(rowArray,cell.getRowOffset(),cell.getRowLength()));
				System.out.println("������"+new String(familyArray,cell.getFamilyOffset(),cell.getFamilyLength()));
				System.out.println("����"+new String(qualifierArray,cell.getQualifierOffset(),cell.getQualifierLength()));
				System.out.println("value"+new String(valueArray,cell.getValueOffset(),cell.getValueLength()));
			}
			
			System.out.println("---------------------");
			
		}
		
	}
	
	
}
