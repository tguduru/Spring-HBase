/**
 * 
 */
package org.bigdata.hbase.spring;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

/**
 * Creates connection to HBase local cluster and create a table if not existed
 * @author ghstreddy
 * 
 */
public class HBaseConnection {

    private final String tableName = "spring_table";

    public HbaseTemplate getHbaseTemplate() {
	ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
		"hbase-beans.xml");
	HbaseTemplate template = (HbaseTemplate) context.getBean("htemplate");
	return template;
    }

    public static void main(String[] args) throws IOException {
	HBaseConnection connection = new HBaseConnection();
	HbaseTemplate hbaseTemplate = connection.getHbaseTemplate();
	HBaseAdmin admin = new HBaseAdmin(hbaseTemplate.getConfiguration());
	connection.createTable(admin);
    }
    public void createTable(HBaseAdmin admin) throws IOException{
	if(!admin.tableExists(tableName)){
	    HTableDescriptor descriptor = new HTableDescriptor(Bytes.toBytes(tableName));
	    HColumnDescriptor column = new HColumnDescriptor(Bytes.toBytes("cf1"));
	    descriptor.addFamily(column);
	    admin.createTable(descriptor);
	    System.out.println("Table Created");
	}
	else
	    System.out.println("Table Already Created");
    }
}
