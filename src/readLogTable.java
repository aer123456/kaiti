/**
 * Created by huguantao on 16/5/21.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.sql.*;

import java.text.SimpleDateFormat;
import java.util.*;

public class readLogTable extends Thread{

    public  String rdb_type;
    public  String rdb_address;
    public  String rdb_username;
    public  String rdb_password;

    public String db_name;
    public readLogTable(String db_name,String rdb_type,String rdb_address,String rdb_username,String rdb_password) {
        this.rdb_type=rdb_type;
        this.rdb_address=rdb_address;
        this.rdb_username=rdb_username;
        this.rdb_password=rdb_password;

        this.db_name=db_name;
    }

    //私有变量 logtable_count记录总的记录数,read_count记录读取中间文件的行数
    private int logtable_count = 0;
    private int read_count = 2;
    private String logTables = "change_log_from_trigger";
    private String filePath = "file/DBFile/";

    public void run() {
        switch(rdb_type) {
            case "mysql":
                int i=1;
                while(i>0) {
                    saveMysqlLog(filePath);
                    try {
                        sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    saveMiddleFile(filePath);
                    save(filePath);
                }
                break;

            case "oracle":
                break;
            case "sqlserver":
                break;
        }
    }

    //存读日志表
    public void saveMysqlLog(String filePath) {
        try {
            //日志表logTable路径,并写入表开头
            File logTable = new File(filePath + "/" + rdb_address + "/" + db_name + "/logTable/log_table.txt");
            if (!logTable.exists()) {
                if(!logTable.createNewFile()) {
                    System.out.println("logTable新建不成功");
                }
                String logTable_head = "id     table_changed     primaryKeyName     primary_key_value     action" +
                        "     table_to     time\n";
                OutputStreamWriter write1 = new OutputStreamWriter(new FileOutputStream(logTable),"UTF-8");
                BufferedWriter writer1 = new BufferedWriter(write1);
                writer1.write(logTable_head);
                writer1.close();
                System.out.println("logTable初始化成功");
            }

            String driver = "com.mysql.jdbc.Driver";
            String url = "jdbc:"+rdb_type+"://"+rdb_address+"/"+db_name+
                    "?useUnicode=true&characterEncoding=utf-8&useSSL=false";

            // 加载驱动程序
            Class.forName(driver);
            // 连接数据库
            Connection conn = DriverManager.getConnection(url, rdb_username, rdb_password);
            if (!conn.isClosed()) {
                System.out.println("Success connecting to logTable Database!");

                // statement用来执行SQL语句
                Statement statement = conn.createStatement();
                Statement statement1 = conn.createStatement();

                //读取日志表的内容并存入文件夹,每次从上次读过的地方开始读取
                String sql = "select * from " + logTables + " where id_s>=" + logtable_count + ";";
                String result = "";
                String blank = "     ";

                ResultSet rs = statement.executeQuery(sql);
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnCount = rsmd.getColumnCount();
                // mark记录单次读取的记录数
                int mark = 0;
                while (rs.next()) {
                    for(int i=1; i<columnCount; i++) {
                        String id = rs.getString("id_s");
                        String table_changed = rs.getString("table_changed");

                        //得到表的主键存入日志表
                        String primaryKey = "";
                        String getPrimkey = "select COLUMN_KEY,COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS" +
                                " where table_name='" + table_changed + "' AND COLUMN_KEY='PRI';";
                        ResultSet primaryKeys = statement1.executeQuery(getPrimkey);
                        while (primaryKeys.next()) {
                            primaryKey = primaryKeys.getString("COLUMN_NAME");
                        }

                        String primary_key = rs.getString("pri_key");
                        String action = rs.getString("action");
                        action =  action.substring(0,action.length()-1);
                        String table_to = rs.getString("table_to");
                        String time = rs.getString("time");
                        result = id + blank + table_changed + blank + primaryKey + blank + primary_key + blank
                                + action + blank + table_to + blank + time + "\n";
                    }

                    //写入日志表
                    OutputStreamWriter write2 = new OutputStreamWriter(new FileOutputStream(logTable,true),"UTF-8");
                    BufferedWriter writer2 = new BufferedWriter(write2);
                    writer2.write(result);
                    writer2.close();
                    System.out.println("结果写入了日志表:"+result);
                    mark = mark + 1;
                }

                logtable_count = logtable_count + mark;
                System.out.println(db_name + "总变化数量:"+logtable_count);
                System.out.println(db_name + "本次变化数量:"+mark);
                System.out.println("------------------------------------");
            }
//-----------------------------关闭连接----------------------------//
            conn.close();



        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //初始化中间文件
    public void initMiddleFile(String filePath) {
        try {
            //日志表文件logTable,中间文件middleFile路径
            File logTable = new File(filePath + "/" + rdb_address + "/" + db_name + "/logTable/log_table.txt");
            File middleFile = new File(filePath + "/" + rdb_address + "/" + db_name + "/middleFile/middleFile.txt");
            if (logTable.exists()&&middleFile.exists()) {
                InputStreamReader read = new InputStreamReader(new FileInputStream(logTable));
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                int mark = 0;
                int line = 0;
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    line++;
                    if(line==1) continue;
                    System.out.println("读出文件:"+lineTxt);
                    String[] arr = lineTxt.split("     ");
                    String id = arr[0];
                    String table_changed = arr[1];
                    String primary_key_name = arr[2];
                    String primary_key_value = arr[3];
                    String action = arr[4];
                    String table_to = arr[5];
                    String time = arr[6];
                    mark = mark + 1;
                    read_count = read_count + mark;

                    String driver = "com.mysql.jdbc.Driver";
                    String url = "jdbc:"+rdb_type+"://"+rdb_address+"/"+db_name+
                            "?useUnicode=true&characterEncoding=utf-8&useSSL=false";

                    //每次读取日志表中的一行,然后开始用这一行的信息来在表中读取
                    String sql = "select * from " + table_changed + " where '" + primary_key_name +
                            "'=" + primary_key_value + ";";

                    System.out.println("i am in init middlefile");
                    // 加载驱动程序
                    Class.forName(driver);
                    // 连接数据库
                    Connection conns = DriverManager.getConnection(url, rdb_username, rdb_password);
                    if (!conns.isClosed()) {
                        System.out.println(db_name + "middleFile 初始化连接建立成功.");

                        // statement用来执行SQL语句
                        Statement statement = conns.createStatement();
                        ResultSet RS = statement.executeQuery(sql);

                        //获取所有的列名存入colums
                        ResultSetMetaData cols = RS.getMetaData();
                        int colNumber =  cols.getColumnCount();
                        String[] colums = new String[colNumber+1];
                        for(int i=1;i<=cols.getColumnCount()-1;i++)
                        {
                            colums[i] = cols.getColumnName(i);
                            System.out.println(colums[i]);
                        }

                        if (!middleFile.exists()) {
                            if(!middleFile.createNewFile()) {
                                System.out.println("middleFile初始化时不存在.");
                            }
                        }

                        String blank = "     ";
                        String middleFile_head = "id" + blank;

                        for (int i=1; i<colums.length; i++) {
                            middleFile_head = middleFile_head + colums[i] + blank;
                        }

                        //给中间文件第一行写进标题
                        middleFile_head = middleFile_head + blank + "action" + blank + "table_to" + blank + "time" + "\n";
                        OutputStreamWriter write1 = new OutputStreamWriter(new FileOutputStream(middleFile),"UTF-8");
                        BufferedWriter writer1 = new BufferedWriter(write1);
                        writer1.write(middleFile_head);
                        writer1.close();
                    }
                    //-----------------------------关闭连接----------------------------//
                    conns.close();
                }
                read.close();

            }else {
                System.out.println("初始化时找不到指定的文件");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    //写中间文件
    public void saveMiddleFile(String filePath) {
        try {
            //日志表文件logTable,中间文件middleFile路径
            File logTable = new File(filePath + "/" + rdb_address + "/" + db_name + "/logTable/log_table.txt");
            File logFile = new File(filePath + "/" + rdb_address + "/" + db_name + "/log/log.txt");
            File middleFile = new File(filePath + "/" + rdb_address + "/" + db_name + "/middleFile/middleFile.txt");
            if (logTable.exists()&&middleFile.exists()&&logFile.exists()) {
                InputStreamReader read = new InputStreamReader(new FileInputStream(logTable));
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                int mark = 0;
                int line = 1;
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    line ++;
                    if(line<=read_count) { continue;}
                    else {
                        String[] arr = lineTxt.split("     ");
                        String id = arr[0];
                        String table_changed = arr[1];
                        String primary_key_name = arr[2];
                        String primary_key_value = arr[3];
                        String action = arr[4];
                        String table_to = arr[5];
                        String time = arr[6];
                        mark = mark + 1;
                        read_count = read_count + mark;

                        String driver = "com.mysql.jdbc.Driver";
                        String url = "jdbc:" + rdb_type + "://" + rdb_address + "/" + db_name +
                                "?useUnicode=true&characterEncoding=utf-8&useSSL=false";

                        //每次读取日志表中的一行,然后开始用这一行的信息来在表中读取
                        String sql = "select * from " + table_changed + " where " + primary_key_name +
                                "=" + primary_key_value + ";";

                        // 加载驱动程序
                        Class.forName(driver);
                        // 连接数据库
                        Connection conn = DriverManager.getConnection(url, rdb_username, rdb_password);
                        if (!conn.isClosed()) {
                            System.out.println(db_name + "的middleFile 存储连接建立成功.");

                            // statement用来执行SQL语句
                            Statement statement = conn.createStatement();
                            ResultSet RS = statement.executeQuery(sql);

                            //获取所有的列名存入colums
                            ResultSetMetaData cols = RS.getMetaData();
                            int colNumber = cols.getColumnCount();
                            String[] colums = new String[colNumber + 1];
                            for (int i = 1; i <= cols.getColumnCount(); i++) {
                                colums[i] = cols.getColumnName(i);
                            }

                            //存进文件内容middleFile
                            while (RS.next()) {
                                String blank = "     ";
                                String middleFile_content = id + blank +time + blank + table_changed
                                         + blank+ action + blank + table_to + blank + primary_key_name + blank;
                                for (int i = 1; i < colums.length; i++) {
                                    middleFile_content = middleFile_content + colums[i] + "__:" + RS.getString(colums[i]) + blank;
                                }
                                middleFile_content = middleFile_content + "\n";

                                //给中间文件写内容
                                OutputStreamWriter write2 = new OutputStreamWriter(new FileOutputStream(middleFile, true), "UTF-8");
                                BufferedWriter writer2 = new BufferedWriter(write2);
                                writer2.write(middleFile_content);
                                writer2.close();
                                System.out.println("写进中间文件内容成功: " + middleFile_content);
                            }
                        }
                        //-----------------------------关闭连接----------------------------//
                        conn.close();

                        //写变更日志log
                        String blank = "     ";
                        String logFile_content = time + blank + table_changed + blank + action + blank + 1
                                + blank + read_count + blank + mark + "\n";
                        OutputStreamWriter write3 = new OutputStreamWriter(new FileOutputStream(logFile, true), "UTF-8");
                        BufferedWriter writer3 = new BufferedWriter(write3);
                        writer3.write(logFile_content);
                        writer3.close();
                        System.out.println("写进中间日志文件内容成功: " + logFile_content);

                    }
                }
                read.close();



            }else {
                System.out.println("找不到指定的文件");
            }
        } catch (SQLException e) {
//            e.printStackTrace();
            System.out.println(" ");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    int save_count = 1;
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
                    if (line <= save_count) {
                        continue;
                    } else {
                        String[] arr = lineTxt.split("     ");
                        String id = arr[0];
                        String time = getTime.format(new java.util.Date());
                        String table_changed = arr[2];
                        String action = arr[3];
                        String to_table = arr[4];
                        String pri_key =  arr[5];

                        String[] content = new String[arr.length];

                        for (int i=6; i<arr.length; i++) {
                            String[] data = arr[i].split("__:");

                            int j = i -6;
                            content[j] = arr[i];

//                            if (isExist(to_table)) {
//                                addRow(to_table,table_changed,pri_key,data[0],data[1]);
//                                mark ++;
//                            } else {
//                                createTable(to_table, content);
//                                addRow(to_table,table_changed,pri_key,data[0],data[1]);
//                                mark ++;
//                            }

                        }
                        save_count += mark;

                        File hbaseLogFile = new File(filePath + "/" + rdb_address + "/" + db_name + "/toHbaseLog/toHbaseLog.txt");
                        String blank = "     ";
                        String logFile_content = time + blank + to_table + blank + pri_key + blank + action + blank + 1
                                + blank + save_count + blank + mark ;
                        OutputStreamWriter write3 = new OutputStreamWriter(new FileOutputStream(hbaseLogFile, true), "UTF-8");
                        BufferedWriter writer3 = new BufferedWriter(write3);
                        writer3.write(logFile_content);
                        writer3.close();
                        System.out.println("同步入hbase并写进hbase同步日志文件内容成功.");
                        System.out.println("总条数: " + save_count + ", tps: " + mark);
                    }
                }
                read.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 声明hbase静态配置
    private static Configuration conf = null;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "60000");
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

    public static void addRow(String tableName, String row,
                              String columnFamily, String column, String value) throws Exception {
        HTable table = new HTable(conf, tableName);
        Put put = new Put(Bytes.toBytes(row));// 指定行
        // 参数分别:列族、列、值
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                Bytes.toBytes(value));
        table.put(put);
    }
}
