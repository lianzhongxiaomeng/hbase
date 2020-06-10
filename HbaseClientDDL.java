package com.dudu.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.Before;
import org.junit.Test;

public class HbaseClientDDL {
	
	Connection conn = null;
	@Before
	public void getConnection() throws Exception {
		// ����һ�����Ӷ���
		Configuration conf = HBaseConfiguration.create();// ���Զ�����hbase��site.xml
		conf.set("hbase.zookeeper.quorum", "hdp-01:2181,hdp-02:2181,hdp-03:2181");
		conn = ConnectionFactory.createConnection(conf);
	}

	/**
	 * DDL
	 * ������
	 * @throws Exception
	 */
	@Test
	public void testCreateTable() throws Exception {

		// �������й���һ��DDL������
		Admin admin = conn.getAdmin();

		// ����һ��������������
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("user_info"));
		// �������嶨����������
		HColumnDescriptor hColumnDescriptor_1 = new HColumnDescriptor("base_info");
		hColumnDescriptor_1.setMaxVersions(3);// ���������д洢���ݵ����汾����Ĭ����1

		HColumnDescriptor hColumnDescriptor_2 = new HColumnDescriptor("extra_info");

		// �����嶨����Ϣ���������������
		hTableDescriptor.addFamily(hColumnDescriptor_1);
		hTableDescriptor.addFamily(hColumnDescriptor_2);

		// ��ddl����������admin������
		admin.createTable(hTableDescriptor);

		admin.close();
		conn.close();

	}
	/**
	 * DDL
	 * ɾ����
	 * @throws Exception
	 */
	@Test
	public void testDropTable() throws Exception {

		Admin admin = conn.getAdmin();
		
		//ͣ�ñ�
		admin.disableTable(TableName.valueOf("user_info"));
		//ɾ����
		admin.deleteTable(TableName.valueOf("user_info"));
		
		admin.close();
		conn.close();
		
	}
	@Test
	public void testAlterTable() throws Exception {
		Admin admin = conn.getAdmin();
		//ȡ���ɵı�����Ϣ
		HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf("user_info"));
		
		//����һ���µ����嶨��
		HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("other_info");
		hColumnDescriptor.setBloomFilterType(BloomType.ROW);//���ø�����Ĳ�¡������
		
		//�������嶨����ӵ����������
		tableDescriptor.addFamily(hColumnDescriptor);
		
		//���޸Ĺ��Ķ��彻����adminȥ�ύ
		admin.modifyTable(TableName.valueOf("user_info"), tableDescriptor);
		admin.close();
		conn.close();
		
	}
	
	
	
}
