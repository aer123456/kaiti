/**
 * Created by huguantao on 16/5/17.
 */

import java.io.*;

import java.sql.*;
import java.util.ArrayList;

public class ConnectMysql extends Thread{
    public  String rdb_type;
    public  String rdb_address;
    public  String rdb_username;
    public  String rdb_password;

    public String db_name;
    public ArrayList<String> tables;

    public ConnectMysql(String db_name,ArrayList<String> tables,String rdb_type,String rdb_address,String rdb_username,String rdb_password) {
        this.rdb_type=rdb_type;
        this.rdb_address=rdb_address;
        this.rdb_username=rdb_username;
        this.rdb_password=rdb_password;

        this.db_name=db_name;
        this.tables=tables;
    }

    @Override  //连接数据库并加载触发器
    public void run() {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:"+rdb_type+"://"+rdb_address+"/"+db_name;
        url = url + "?useUnicode=true&characterEncoding=utf-8&useSSL=false";

        try {
            // 加载驱动程序
            Class.forName(driver);
            // 连接数据库
            Connection conn = DriverManager.getConnection(url, rdb_username, rdb_password);
            if(!conn.isClosed()) {
                System.out.println("Success connecting to Database!");

                // statement用来执行SQL语句
                Statement statement = conn.createStatement();

                //新建一个日志表
                if (statement.executeUpdate("drop table if exists change_log_from_trigger;") != 0) {
                    System.out.println(db_name + "上的旧日志表删除失败");
                }
                String create_log_table = "create table if not exists change_log_from_trigger(" +
                        "id_s int(11) primary key not null auto_increment," +
                        "table_changed varchar(30),pri_key varchar(30)," +
                        "action varchar(30),table_to varchar(30)," +
                        "time timestamp default current_timestamp);";
                if (statement.executeUpdate(create_log_table) == 0) {
                    System.out.println(db_name + "上的日志表新建成功");
                } else {
                    System.out.println(db_name + "上的日志表新建失败");
                }


                for (int i = 0; i < tables.size(); i++) {
                    //该db下面的所有表和相关信息
                    String[] table_param = tables.get(i).split("&&");

                    //获取表名,同步到hbase哪张表中,以及监控时间.
                    for (int j = 0; j < table_param.length / 3; j++) {
                        String table_name = table_param[j * 3];
                        String table_to = table_param[j*3+1];
                        String time_space = table_param[j*3+2];

                        // 查看表并建立相关文件夹和触发器
                        DatabaseMetaData dmd2 = conn.getMetaData();
                        ResultSet table_rs = dmd2.getTables(null, null, null, null);
                        while (table_rs.next()) {
                            String tt2 = table_rs.getString("TABLE_NAME");
                            if (tt2.equals(table_name)) {
                                //建立文件夹
                                String filepath = "file/DBFile/";
                                if ((makeDir(filepath, rdb_address, db_name, table_name)) == true) {
                                    System.out.println("存储文件夹和相应文件创建成功.");
                                } else {
                                    System.out.println("存储文件夹和相应文件创建失败");
                                }

                                //得到表的主键
                                String primaryKey = "";
                                String getPrimkey = "select COLUMN_KEY,COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS" +
                                        " where table_name='" + table_name + "' AND COLUMN_KEY='PRI';";
                                ResultSet primaryKeys = statement.executeQuery(getPrimkey);
                                while (primaryKeys.next()) {
                                    primaryKey = primaryKeys.getString("COLUMN_NAME");
                                }

                                //创建update insert DELETE触发器
                                String[] update_trigger = makeTrigger(primaryKey, table_name, "update", table_to);

                                if(statement.executeUpdate(update_trigger[0]) != 0){
                                    System.out.println("database:" + db_name + "上的" + table_name + "表删除旧的update触发器失败");
                                }
                                if (statement.executeUpdate(update_trigger[1]) != 0) {
                                    System.out.println("database:" + db_name + "上的" + table_name + "表添加新的update触发器失败");
                                }
                                String[] insert_trigger = makeTrigger(primaryKey, table_name, "insert", table_to);
                                if(statement.executeUpdate(insert_trigger[0]) != 0){
                                    System.out.println("database:" + db_name + "上的" + table_name + "表删除旧的insert触发器失败");
                                }
                                if (statement.executeUpdate(insert_trigger[1]) != 0) {
                                    System.out.println("database:" + db_name + "上的" + table_name + "表添加新的insert触发器失败");
                                }
                                String[] delete_trigger = makeTrigger(primaryKey, table_name, "delete", table_to);
                                if(statement.executeUpdate(delete_trigger[0]) != 0){
                                    System.out.println("database:" + db_name + "上的" + table_name + "表删除旧的delete触发器失败");
                                }
                                if (statement.executeUpdate(delete_trigger[1]) != 0) {
                                    System.out.println("database:" + db_name + "上的" + table_name + "表添加新的delete触发器失败");
                                }
                                System.out.println("database:" + db_name + "上的所有trigger 创建成功,并已开始监控.");
                            }
                        }
                    }
                }
            }
            //-----------------------------关闭连接----------------------------//
            conn.close();


        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //新建表的所有相关文件夹和文件
    public static boolean makeDir(String filePath,String rdb_address,String db_name,String table_name) {
        try {
            File backup = new File(filePath + "/" + rdb_address + "/" + db_name + "/" + table_name + "/backup");
            File log = new File(filePath + "/" + rdb_address + "/" + db_name + "/" + table_name + "/log");
            File middleFile = new File(filePath + "/" + rdb_address + "/" + db_name + "/" + table_name + "/middleFile");
            File toHbaseLog = new File(filePath + "/" + rdb_address + "/" + db_name + "/" + table_name + "/toHbaseLog");

            if (backup.exists() && log.exists() && middleFile.exists() && toHbaseLog.exists()) {
                File log_file = new File(log.getAbsolutePath(),"log.txt");
                if(!log_file.exists()) {
                    if (!log_file.createNewFile()) {
                        System.out.println(rdb_address + "下的" + db_name + "数据库" + "的表" + table_name + "生成log文件夹和文件失败");
                        return true;
                    }
                }

                File middleFile_file = new File(middleFile.getAbsolutePath(), "middleFile.txt");
                if (!middleFile.exists()) {
                    if (!middleFile_file.createNewFile()) {
                        System.out.println(rdb_address + "下的" + db_name + "数据库" + "的表" + table_name + "生成middleFile文件夹和文件失败");
                        return true;
                    }
                }

                File toHbaseLog_file = new File(toHbaseLog.getAbsolutePath(),"toHbaseLog.txt");
                if (!toHbaseLog_file.exists()) {
                    if (!toHbaseLog_file.createNewFile()) {
                        System.out.println(rdb_address + "下的" + db_name + "数据库" + "的表" + table_name + "生成toHbaseLog文件夹和文件失败");
                        return true;
                    }
                }

                return false;

            } else {
                boolean mkdir_result = backup.mkdirs() && log.mkdirs() && middleFile.mkdirs() && toHbaseLog.mkdirs();
                if(mkdir_result) {
                    File log_file = new File(log.getAbsolutePath(),"log.txt");
                    if(!log_file.exists()) {
                        if (!log_file.createNewFile()) {
                            System.out.println(rdb_address + "下的" + db_name + "数据库" + "的表" + table_name + "生成log文件夹和文件失败");
                            return true;
                        }
                    }

                    File middleFile_file = new File(middleFile.getAbsolutePath(), "middleFile.txt");
                    if (!middleFile.exists()) {
                        if (!middleFile_file.createNewFile()) {
                            System.out.println(rdb_address + "下的" + db_name + "数据库" + "的表" + table_name + "生成middleFile文件夹和文件失败");
                            return true;
                        }
                    }

                    File toHbaseLog_file = new File(toHbaseLog.getAbsolutePath(),"toHbaseLog.txt");
                    if (!toHbaseLog_file.exists()) {
                        if (!toHbaseLog_file.createNewFile()) {
                            System.out.println(rdb_address + "下的" + db_name + "数据库" + "的表" + table_name + "生成toHbaseLog文件夹和文件失败");
                            return true;
                        }
                    }

                    return false;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  true;
    }

    //生成sql语句:创建一个trigger并设置存储入日志表
    public static String[] makeTrigger(String primary_key, String table_name, String trigger_type, String table_to){
        String[] sql = new String[2];
        String drop_existed_Trigger = "drop trigger if exists " + trigger_type + "_" + table_name + ";";
        sql[0] = drop_existed_Trigger;

        if(trigger_type.equals("update")) {
            String create_updateTrigger = "create trigger " + trigger_type + "_" + table_name +
                    " after " + trigger_type + " on " + table_name + " for each row ";
            String trigger_content = "begin " +
                    "insert into change_log_from_trigger(id_s,table_changed,pri_key,action,table_to,time)" +
                    " values(null,'" + table_name + "',new." + primary_key + ",'" + trigger_type + "s','" + table_to + "',null); end;";
            sql[1] = create_updateTrigger + trigger_content;
        }else if(trigger_type.equals("insert")) {
            String create_updateTrigger = "create trigger " + trigger_type + "_" + table_name +
                    " after " + trigger_type + " on " + table_name + " for each row ";
            String trigger_content = "begin " +
                    "insert into change_log_from_trigger(id_s,table_changed,pri_key,action,table_to,time)" +
                    " values(null,'" + table_name + "',mew." + primary_key + ",'" + trigger_type + "s','" + table_to + "',null); end;";
            sql[1] = create_updateTrigger + trigger_content;
        }
        else if(trigger_type.equals("delete")) {
            String create_updateTrigger = "create trigger " + trigger_type + "_" + table_name +
                    " before " + trigger_type + " on " + table_name + " for each row ";
            String trigger_content = "begin " +
                    "insert into change_log_from_trigger(id_s,table_changed,pri_key,action,table_to,time)" +
                    " values(null,'" + table_name + "',old." + primary_key + ",'" + trigger_type + "s','" + table_to + "',null); end;";
            sql[1] = create_updateTrigger + trigger_content;
        }
        return sql;
    }
}
