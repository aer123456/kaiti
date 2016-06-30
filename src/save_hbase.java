/**
 * Created by huguantao on 16/5/21.
 */

import java.io.*;
import java.util.List;
import java.util.ArrayList;

import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

//本机的hbase:  /usr/local/Cellar/hbase/1.1.2/libexec/bin
// 启动:进入上述目录后:   sh start-hbase.sh 即可启动, 然后进入hbase shell: hbase shell

public class save_hbase extends Thread{
    public  String rdb_type;
    public  String rdb_address;
    public  String rdb_username;
    public  String rdb_password;
    public String db_name;

    public String hbase_address;
    public String hbase_username;
    public String hbase_password;

    public save_hbase(String db_name,String rdb_type,String rdb_address,String rdb_username,String rdb_password,String hbase_address,String hbase_username,String hbase_password) {
        this.rdb_type=rdb_type;
        this.rdb_address=rdb_address;
        this.rdb_username=rdb_username;
        this.rdb_password=rdb_password;
        this.db_name=db_name;

        this.hbase_address = hbase_address;
        this.hbase_username = hbase_username;
        this.hbase_password = hbase_password;
    }

    private String filePath = "file/DBFile/";

    public void run() {
        int i=1;
        while(i>0) {
            try {
                sleep(15000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            save(filePath);
        }
    }

    // 声明hbase静态配置
    private static Configuration conf = null;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        conf.set("hbase.zookeeper.property.clientPort", "6666");
    }

    //用于记录总得读取数目
    int read_count = 1;

    public void save(String filePath) {
        File middleFile = new File(filePath + "/" + rdb_address + "/" + db_name + "/middleFile/middleFile.txt");
        try {
            if (middleFile.exists() && middleFile.length() > 0) {
                InputStreamReader read = new InputStreamReader(new FileInputStream(middleFile));
                BufferedReader bufferedReader = new BufferedReader(read);
                SimpleDateFormat getTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String lineTxt = null;
                int mark = 0;
                int line = 0;
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    line ++;
                    if (line <= read_count) {
                        continue;
                    } else {
                        String[] arr = lineTxt.split("     ");
                        String id = arr[0];
                        String time = getTime.format(new Date());
                        String table_changed = arr[2];
                        String action = arr[3];
                        String to_table = arr[4];
                        String pri_key =  arr[5];

                        String[] content = new String[arr.length];

                        for (int i=6; i<arr.length; i++) {
                            String[] data = arr[i].split("__:");

                            int j = i -6;
                            content[j] = arr[i];
                            System.out.println(arr[i]);
                            System.out.println("--------------------------");

                            if (isExist(to_table)) {
                                addRow(to_table,table_changed,pri_key,data[0],data[1]);
                                mark ++;
                            } else {
                                createTable(to_table, content);
                                addRow(to_table,table_changed,pri_key,data[0],data[1]);
                                mark ++;
                            }
                        }
                        read_count += mark;

                        File hbaseLogFile = new File(filePath + "/" + rdb_address + "/" + db_name + "/toHbaseLog/toHbaseLog.txt");
                        String blank = "     ";
                        String logFile_content = time + blank + to_table + blank + pri_key + blank + action + blank + 1
                                + blank + read_count + blank + mark ;
                        OutputStreamWriter write3 = new OutputStreamWriter(new FileOutputStream(hbaseLogFile, true), "UTF-8");
                        BufferedWriter writer3 = new BufferedWriter(write3);
                        writer3.write(logFile_content);
                        writer3.close();
                        System.out.println("同步入hbase并写进hbase同步日志文件内容成功.");
                        System.out.println("总条数: " + read_count + ", tps: " + mark);
                    }
                }
                read.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    //判断表是否存在
    public static boolean isExist(String tableName) throws Exception {
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        return hAdmin.tableExists(tableName);
    }

    // 创建数据库表
    public static void createTable(String tableName, String[] columnFamilys)
            throws Exception {
        // 新建一个数据库管理员
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        if (hAdmin.tableExists(tableName)) {
            System.out.println("表 "+tableName+" 已存在！");
        } else {
            // 新建一个表的描述
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            // 在表的描述里添加列族
            for (String columnFamily : columnFamilys) {
                tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            }
            // 根据配置好的描述建表
            hAdmin.createTable(tableDesc);
            System.out.println("创建表 "+tableName+" 成功!");
        }
    }

    // 删除数据库表
    public static void deleteTable(String tableName) throws Exception {
        // 新建一个数据库管理员
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        if (hAdmin.tableExists(tableName)) {
            // 关闭一个表
            hAdmin.disableTable(tableName);
            hAdmin.deleteTable(tableName);
            System.out.println("删除表 "+tableName+" 成功！");
        } else {
            System.out.println("删除的表 "+tableName+" 不存在！");
            System.exit(0);
        }
    }

    // 添加一条数据
    public static void addRow(String tableName, String row,
                              String columnFamily, String column, String value) throws Exception {
        HTable table = new HTable(conf, tableName);
        Put put = new Put(Bytes.toBytes(row));// 指定行
        // 参数分别:列族、列、值
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                Bytes.toBytes(value));
        table.put(put);
    }

    // 删除一条(行)数据
    public static void delRow(String tableName, String row) throws Exception {
        HTable table = new HTable(conf, tableName);
        Delete del = new Delete(Bytes.toBytes(row));
        table.delete(del);
    }

    // 删除多条数据
    public static void delMultiRows(String tableName, String[] rows)
            throws Exception {
        HTable table = new HTable(conf, tableName);
        List<Delete> delList = new ArrayList<Delete>();
        for (String row : rows) {
            Delete del = new Delete(Bytes.toBytes(row));
            delList.add(del);
        }
        table.delete(delList);
    }

    // 获取一条数据
    public static void getRow(String tableName, String row) throws Exception {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(row));
        Result result = table.get(get);
        // 输出结果,raw方法返回所有keyvalue数组
        for (KeyValue rowKV : result.raw()) {
            System.out.print("行名:" + new String(rowKV.getRow()) + " ");
            System.out.print("时间戳:" + rowKV.getTimestamp() + " ");
            System.out.print("列族名:" + new String(rowKV.getFamily()) + " ");
            System.out.print("列名:" + new String(rowKV.getQualifier()) + " ");
            System.out.println("值:" + new String(rowKV.getValue()));
        }
    }

    // 获取所有数据
    public static void getAllRows(String tableName) throws Exception {
        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        // 输出结果
        for (Result result : results) {
            for (KeyValue rowKV : result.raw()) {
                System.out.print("行名:" + new String(rowKV.getRow()) + " ");
                System.out.print("时间戳:" + rowKV.getTimestamp() + " ");
                System.out.print("列族名:" + new String(rowKV.getFamily()) + " ");
                System.out
                        .print("列名:" + new String(rowKV.getQualifier()) + " ");
                System.out.println("值:" + new String(rowKV.getValue()));
            }
        }
    }

}
