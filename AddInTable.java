import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class AddInTable {

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HTable htable = new HTable(conf, "org");
        System.out.println("Запись организаций..");
        int oid = 1;
        byte[] rowkey = Bytes.toBytes(oid);
        Put put = new Put(rowkey);
        put.addColumn(Bytes.toBytes("oid"), Bytes.toBytes("title"), Bytes.toBytes("Asus"));
        htable.put(put);
        oid = 2;
        rowkey = Bytes.toBytes(oid);
        put = new Put(rowkey);
        put.addColumn(Bytes.toBytes("oid"), Bytes.toBytes("title"), Bytes.toBytes("Samsung"));
        htable.put(put);
        htable.flushCommits();
        htable.close();

        htable = new HTable(conf, "bill");
        System.out.println("Запись чеков..");
        oid = 1;
        rowkey = Bytes.toBytes(oid);
        put = new Put(rowkey);
        put.addColumn(Bytes.toBytes("bid"), Bytes.toBytes("data"), Bytes.toBytes("2"));
        htable.put(put);
        oid = 2;
        rowkey = Bytes.toBytes(oid);
        put = new Put(rowkey);
        put.addColumn(Bytes.toBytes("bid"), Bytes.toBytes("data"), Bytes.toBytes("2"));
        htable.put(put);
        oid = 3;
        rowkey = Bytes.toBytes(oid);
        put = new Put(rowkey);
        put.addColumn(Bytes.toBytes("bid"), Bytes.toBytes("data"), Bytes.toBytes("1"));
        htable.put(put);
        oid = 4;
        rowkey = Bytes.toBytes(oid);
        put = new Put(rowkey);
        put.addColumn(Bytes.toBytes("bid"), Bytes.toBytes("data"), Bytes.toBytes("1"));
        htable.put(put);

        htable.flushCommits();
        htable.close();

        System.out.println("Готово");
    }
}
