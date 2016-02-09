import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.conf.Configuration;

public class BillSystem {

    public static void main(String[] args) throws IOException {
        // Создаем концигурации
        Configuration con = HBaseConfiguration.create();
        // Создаем админа
        HBaseAdmin admin = new HBaseAdmin(con);

 /*       // Создаем таблицу Чек
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("bill"));
        // Добавляем колонки id, дата, id организации
        tableDescriptor.addFamily(new HColumnDescriptor("bid"));
        tableDescriptor.addFamily(new HColumnDescriptor("date"));
        tableDescriptor.addFamily(new HColumnDescriptor("oid"));
        admin.createTable(tableDescriptor);*/

        // Создаем таблицу Содержимое чека
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("billcontent"));
        // Добавляем колонки товар, кол-во, цена
        tableDescriptor.addFamily(new HColumnDescriptor("bid"));
        tableDescriptor.addFamily(new HColumnDescriptor("good"));
        tableDescriptor.addFamily(new HColumnDescriptor("price"));
        admin.createTable(tableDescriptor);
/*
        // Создаем таблицу Органицация
        tableDescriptor = new HTableDescriptor(TableName.valueOf("org"));
        // Добавляем колонки id  наименование организации title
        tableDescriptor.addFamily(new HColumnDescriptor("oid"));
        tableDescriptor.addFamily(new HColumnDescriptor("title"));
        admin.createTable(tableDescriptor);

        // Создаем таблицу Результат
        tableDescriptor = new HTableDescriptor(TableName.valueOf("join_result"));
        // Добавляем колонки id  наименование организации title
        tableDescriptor.addFamily(new HColumnDescriptor("bid"));
        tableDescriptor.addFamily(new HColumnDescriptor("count"));
        admin.createTable(tableDescriptor);*/

        System.out.println("Таблицы созданы");
    }
}