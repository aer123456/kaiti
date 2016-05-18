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

        try {
            // 加载驱动程序
            Class.forName(driver);
            // 连续数据库
            Connection conn = DriverManager.getConnection(url, rdb_username, rdb_password);
            if(!conn.isClosed())
                System.out.println("Success connecting to Database!");

            // statement用来执行SQL语句
            Statement statement = conn.createStatement();

            //新建一个日志表
            String create_log_table = "create table if not exists change_log_from_trigger(" +
                    "id_s int(11) primary key auto_increment," +
                    "table_changed varchar(30),row_changed varchar(30)," +
                    "action varchar(30),value varchar(200));";
            boolean create_log_table_rs = statement.execute(create_log_table);
//            if (create_log_table_rs) {
                System.out.println("日志表新建成功");
//            } else {
//                System.out.println("日志表新建失败");
//            }


            for(int i=0;i<tables.size();i++) {
                //该db下面的所有表和相关信息
                String [] table_param = tables.get(i).split("&&");
                for(int j=0;j<table_param.length/3;j++) {
                    String table_name = table_param[j*3];

                    // 查看表并建立相关文件夹
                    DatabaseMetaData dmd2 = conn.getMetaData();
                    ResultSet table_rs = dmd2.getTables(null,null,null,null);
                    while(table_rs.next()) {
                        String tt2=table_rs.getString("TABLE_NAME");
                        if(tt2.equals(table_name)){
                            //建立文件夹
                            String filepath = "file/DBFile/";
                            if((makeDir(filepath,rdb_address,db_name,table_name)) == true) {
                                System.out.println("存储文件夹创建成功.");
                            }else {
                                System.out.println("存储文件夹创建失败");
                            }
                        }
                    }

                    //创建update触发器
                    String update_trigger = makeTrigger(table_name, "update");
                    System.out.println(update_trigger);
                    boolean update_trigger_rs = statement.execute(update_trigger);
                    if(update_trigger_rs){
                        System.out.println("update trigger 添加成功,已经开始监控.");
                    } else {
                        System.out.println("update trigger 添加失败.");
                    }

//                    //创建insert触发器
//                    String insert_trigger = makeTrigger(table_name, "update");
//                    ResultSet insert_trigger_rs = statement.executeQuery(insert_trigger);
//                    if(!insert_trigger_rs.equals(null)){
//                        System.out.println("insert trigger 添加成功,已经开始监控.");
//                    } else {
//                        System.out.println("insert trigger 添加失败.");
//                    }
//
//                    //创建insert触发器
//                    String delete_trigger = makeTrigger(table_name, "update");
//                    ResultSet delete_trigger_rs = statement.executeQuery(delete_trigger);
//                    if(!delete_trigger_rs.equals(null)){
//                        System.out.println("insert trigger 添加成功,已经开始监控.");
//                    } else {
//                        System.out.println("insert trigger 添加失败.");
//                    }
                }
            }

            //关闭连接
            //conn.close();


        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //新建表的所有相关文件夹
    public static boolean makeDir(String filePath,String rdb_address,String db_name,String table_name) {
        File backup = new File(filePath+"/"+rdb_address + "/" + db_name + "/" + table_name + "/backup");
        File log = new File(filePath+"/"+rdb_address + "/" + db_name + "/" + table_name + "/log");
        File logTable = new File(filePath+"/"+rdb_address + "/" + db_name + "/" + table_name + "/logTable");
        File middleFile = new File(filePath+"/"+rdb_address + "/" + db_name + "/" + table_name + "/middleFile");
        File toHbaseLog = new File(filePath+"/"+rdb_address + "/" + db_name + "/" + table_name + "/toHbaseLog");

        if(backup.exists()&&log.exists()&&logTable.exists()&&middleFile.exists()&&toHbaseLog.exists())
        {
            return true;
        }
        else
        {
            boolean mkdir_result = backup.mkdirs()&&log.mkdirs()&&logTable.mkdirs()&&middleFile.mkdirs()&&toHbaseLog.mkdirs();
            return mkdir_result;
        }
    }

    //创建一个trigger并设置存储入日志表
    public static String makeTrigger(String table_name, String trigger_type){
        String sql = "";
        if(trigger_type.equals("update") || getAllStackTraces().equals("insert")) {
//            String drop_existed_Trigger = "drop trigger if exists " + trigger_type + "_" + table_name + ";";
            String create_updateTrigger = "create trigger " + trigger_type + "_" + table_name +
                    " after " + trigger_type + " on " + table_name + " for each row ";
            String trigger_content = "begin select * from new;end;";
            sql = create_updateTrigger + trigger_content;
        } else if(trigger_type.equals("delete")) {
            String drop_existed_Trigger = "drop trigger if exists " + trigger_type + "_" + table_name + ";";
            String create_updateTrigger = "create trigger " + trigger_type + "_" + table_name +
                    " after " + trigger_type + " on " + table_name + " for each row ";
            String trigger_content = "begin select * from old;end;";
            sql = drop_existed_Trigger + create_updateTrigger + trigger_content;
        }
        return sql;
    }
}
