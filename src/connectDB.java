/**
 * Created by huguantao on 16/5/15.
 */
import java.lang.String;
import java.io.*;
import java.lang.System;
import java.util.ArrayList;
import javax.xml.parsers.*;
import org.w3c.dom.*;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


public class connectDB {

    public static void main (String args[]) {

        //拿到hbase的配置参数
        String[] hbase_config = new String[3];
        hbase_config = read_hbase_config();

        //拿到RDB的配置参数
        ArrayList rdb_config = new ArrayList();
        rdb_config = read_rdbs_config();

        System.out.println(rdb_config.size());
    }

    //获取hbase配置文件信息
    public static String[] read_hbase_config() {
        String[] hbase_config = new String[3];
        try{
            File configFile = new File("file/configs/config.xml");
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document configs = builder.parse(configFile);

            NodeList Hbase = configs.getElementsByTagName("HBASE");
            if (Hbase != null) {
                for(int i = 0; i < Hbase.getLength(); i++) {
                    Element element = (Element) Hbase.item(i);
                    String hbase_address = element.getElementsByTagName("address").item(0).getFirstChild().getNodeValue();
                    String hbase_username = element.getElementsByTagName("username").item(0).getFirstChild().getNodeValue();
                    String hbase_password = element.getElementsByTagName("password").item(0).getFirstChild().getNodeValue();
                    hbase_config[0] = hbase_address;
                    hbase_config[1] = hbase_username;
                    hbase_config[2] = hbase_password;
                }
            } else {
                System.out.print("配置文件中HBASE部分错误,请重新配置.");
                System.exit(0);
            }

        }catch(Exception e){
            e.printStackTrace();
        }
        return hbase_config;
    }

    //获取RDBS配置文件信息
    public static ArrayList read_rdbs_config() {
        ArrayList all_database = new ArrayList();
        try{
            File configFile = new File("file/configs/config.xml");
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document configs = builder.parse(configFile);
            NodeList RDB = configs.getElementsByTagName("RDB");

            if(RDB != null) {
                String[] rdb_config = new String[4];
                for(int i = 0; i < RDB.getLength(); i++) {
                    Element element = (Element) RDB.item(i);
                    //得到类型,地址,用户名,密码并存入数组 rdb_config
                    String rdb_type = element.getElementsByTagName("type").item(0).getFirstChild().getNodeValue();
                    String rdb_address = element.getElementsByTagName("address").item(0).getFirstChild().getNodeValue();
                    String rdb_username = element.getElementsByTagName("username").item(0).getFirstChild().getNodeValue();
                    String rdb_password = element.getElementsByTagName("password").item(0).getFirstChild().getNodeValue();
                    rdb_config[0] = rdb_type;
                    rdb_config[1] = rdb_address;
                    rdb_config[2] = rdb_username;
                    rdb_config[3] = rdb_password;

                    //将存储类型,地址,用户名,密码的数组 rdb_config存入ArrayList all_database中
                    for (int l=0; l<rdb_config.length; l++){
                        all_database.add(rdb_config[l]);
                    }


                    NodeList databases = element.getElementsByTagName("database");

                    //遍历所有database
                    for (int j=0; j<databases.getLength(); j++) {

                        Element table = (Element) databases.item(j);
                        String db_name = table.getElementsByTagName("name").item(0).getFirstChild().getNodeValue();

                        //得到当前database下所有的table
                        NodeList table_all = table.getElementsByTagName("table");
                        ArrayList tables = new ArrayList();
                        tables.add(db_name);
                        for(int k=0; k<table_all.getLength(); k++){
                            Element table_each = (Element) table_all.item(k);
                            String table_name = table_each.getElementsByTagName("tablename").item(0).getFirstChild().getNodeValue();
                            String table_sync = table_each.getElementsByTagName("sync").item(0).getFirstChild().getNodeValue();
                            String table_timeSpace = table_each.getElementsByTagName("timespace").item(0).getFirstChild().getNodeValue();
                            tables.add(table_name + "&&" + table_sync + "&&" + table_timeSpace);
                        }
                        all_database.addAll(tables);
                    }
                }
            }else {
                System.out.print("配置文件中RDB部分错误,请重新配置.");
                System.exit(0);
            }

        }catch(Exception e){
            e.printStackTrace();
        }
        return all_database;
    }


    public static void cnnoectDB() {

    }

}

