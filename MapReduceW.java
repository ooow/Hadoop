import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

public class MapReduceW {

    static class Mapper extends TableMapper<ImmutableBytesWritable, Put> {

        // из 2-х таблиц выстраивает все данные по единому ключу oid (по которому происходит слияние join)
        @Override
        public void map(ImmutableBytesWritable row, Result values, Context context)
                throws IOException, InterruptedException {
            try {
                byte[] tableName = ((TableSplit) context.getInputSplit()).getTableName(); // имя таблички

                // пользуемся свойствами наших данных и берем только первую запись.
                Cell kv = values.listCells().get(0);
                byte[] family = Bytes.copy(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength());

                System.out.println("Checking table '" + Bytes.toString(tableName) + "', family '" + Bytes.toString(family) + "'");

                ImmutableBytesWritable userKey;
                Put value;

                if (Bytes.compareTo(family, Bytes.toBytes("oid")) == 0) {
                    userKey = new ImmutableBytesWritable(row.get(), 0, Bytes.SIZEOF_INT);
                    value = new Put(Bytes.toBytes("vendor")); //маркер
                    value.addColumn(Bytes.toBytes("title"), null, Bytes.copy(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()));
                } else if (Bytes.compareTo(family, Bytes.toBytes("bid")) == 0) {
                    userKey = new ImmutableBytesWritable(Bytes.toBytes(Integer.parseInt(Bytes.toString(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()))));
                    value = new Put(Bytes.toBytes("bill"));  //маркер
                    value.addColumn(Bytes.toBytes("bid"), null, Bytes.copy(row.get(), 0, Bytes.SIZEOF_INT));
                } else {
                    return;
                }
                context.write(userKey, value);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    // перемножение таблиц по ключу
    public static class Reducer extends TableReducer<ImmutableBytesWritable, Put, ImmutableBytesWritable> {
        static public int index = 0;

        public void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context)
                throws IOException, InterruptedException {
            // сюда поподают values с одним и тем же oid из двух таблиц
            List<Put> vendors = new ArrayList<Put>();
            List<Put> bills = new ArrayList<Put>();

            try {
                // Не будем полагаться что список упорядочен по мере прохождения, сделаем два прохода.
                for (Put val : values) {
                    if (Bytes.compareTo(val.getRow(), Bytes.toBytes("vendor")) == 0) {
                        vendors.add(val);
                    }
                    if (Bytes.compareTo(val.getRow(), Bytes.toBytes("bill")) == 0)
                        bills.add(val);
                }

                for (Put val : bills) {
                    Cell bid = val.get(Bytes.toBytes("bid"), null).get(0);
                    for (Put vendor : vendors) {
                        ImmutableBytesWritable newKey = new ImmutableBytesWritable(Bytes.toBytes(index++));
                        Cell title = vendor.get(Bytes.toBytes("title"), null).get(0);
                        Put newVal = new Put(newKey.copyBytes());
                        newVal.addColumn(Bytes.toBytes("bid"), null, Bytes.copy(bid.getValueArray(), bid.getValueOffset(), bid.getValueLength()));
                        newVal.addColumn(Bytes.toBytes("title"), null, Bytes.copy(title.getValueArray(), title.getValueOffset(), title.getValueLength()));
                        context.write(newKey, newVal);
                        System.out.println("bid:" + Bytes.toInt(bid.getValueArray(), bid.getValueOffset(), bid.getValueLength()) +
                                " title:" + Bytes.toString(title.getValueArray(), title.getValueOffset(), title.getValueLength()));
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        List scans = new ArrayList();
        Configuration conf = HBaseConfiguration.create();
        Job job = null;
        try {
            job = Job.getInstance(conf, "Hbase_MapReduceW");
        } catch (IOException e) {
            System.out.println("connect erorre");
        }
        job.setJarByClass(MapReduceW.class);

        System.out.println("please enter new table name: ");
        Scanner sc = new Scanner(System.in);
        String result = sc.next();
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(result));

            hTableDescriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("bid")));
            hTableDescriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("title")));

            admin.createTable(hTableDescriptor);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        String t1 = "org";
        String t2 = "bill";

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("oid"));
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, t1.getBytes());
        scan.setFilter(new FirstKeyOnlyFilter());
        scans.add(scan);

        Scan scan2 = new Scan();
        scan2.addFamily(Bytes.toBytes("bid"));
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, t2.getBytes());
        scan2.setFilter(new FirstKeyOnlyFilter());
        scans.add(scan2);

        try {
            TableMapReduceUtil.initTableMapperJob(scans, Mapper.class, ImmutableBytesWritable.class, Put.class, job);
            TableMapReduceUtil.initTableReducerJob(result, Reducer.class, job);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException ignored) {
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}

/*
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;


public class MapReduceW {

    static class Mapper extends TableMapper<ImmutableBytesWritable, IntWritable> {
        private static final IntWritable one = new IntWritable(1);

        @Override
        public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException, InterruptedException {
            // сгрупировка (ключам) по значениям и присваивает им единичку
            ImmutableBytesWritable a = new ImmutableBytesWritable(Bytes.toBytes(Integer.parseInt(Bytes.toString(values.value()))));
            context.write(a, one);
        }
    }

    public static class Reducer extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {

        public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            Put put = new Put(key.get());
            put.addColumn(Bytes.toBytes("bid"), null, key.get());
            put.addColumn(Bytes.toBytes("count"), null, Bytes.toBytes(sum));
            System.out.println("bid: " + Bytes.toInt(key.get()) + "  count: " + sum);
            context.write(key, put);
        }
    }

    public static void main(String[] args) {
        List scans = new ArrayList();
        Configuration conf = HBaseConfiguration.create();
        Job job = null;
        try {
            job = Job.getInstance(conf, "Hbase_MapReduceW");
        } catch (IOException e) {
            System.out.println("connect erorre");
        }
        job.setJarByClass(MapReduceW.class);

        String t2 = "billcontent";

        Scan scan2 = new Scan();
        scan2.addFamily(Bytes.toBytes("bid"));
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, t2.getBytes());
        scans.add(scan2);

        try {
            TableMapReduceUtil.initTableMapperJob(scans, Mapper.class, ImmutableBytesWritable.class, IntWritable.class, job);
            TableMapReduceUtil.initTableReducerJob("join_result", Reducer.class, job);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException ignored) {
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}*/


