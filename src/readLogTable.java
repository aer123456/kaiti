/**
 * Created by huguantao on 16/5/21.
 */
import java.io.*;
import java.sql.*;

import java.util.Timer;
import java.util.TimerTask;

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

    //私有变量 count记录总的记录数
    private int count = 0;
    private String logTable = "change_log_from_trigger";


    public void run() {
        switch(rdb_type) {
            case "mysql":

                Timer timer = new Timer();
                timer.schedule(new TimerTask() {
                    public void run() {
                        readMysqlLog();
                    }
                }, 10000 , 15000);  //10s后开始执行,每10s执行一次
                break;
            case "oracle":
                break;
            case "sqlserver":
                break;
        }
    }

    public void readMysqlLog() {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:"+rdb_type+"://"+rdb_address+"/"+db_name;
        url = url + "?useUnicode=true&characterEncoding=utf-8&useSSL=false";

        try {
            // 加载驱动程序
            Class.forName(driver);
            // 连接数据库
            Connection conn = DriverManager.getConnection(url, rdb_username, rdb_password);
            if (!conn.isClosed()) {
                System.out.println("Success connecting to Database!");

                // statement用来执行SQL语句
                Statement statement = conn.createStatement();

                //读取日志表的内容并存入文件夹,每次从上次读过的地方开始读取
                String sql = "select * from " + logTable + " where id_s>" + count + ";";
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
                        String primary_key = rs.getString("pri_key");
                        String action = rs.getString("action");
                        String table_to = rs.getString("table_to");
                        String time = rs.getString("time");
                        result = id + blank + table_changed + blank + primary_key + blank
                                + action + blank + table_to + blank + time + "\n";
                    }
                    System.out.println("结果:"+result);
                    mark = mark + 1;
                }
                count = count + mark;
                System.out.println(db_name + "总数量:"+count);
                System.out.println(db_name + "本次数量:"+mark);
            }


            //-----------------------------关闭连接----------------------------//
            conn.close();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
