/**
 * Created by huguantao on 16/5/15.
 */
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.ArrayList;

public class connectDB extends Thread{

    //hbase配置参数私有变量
    private String hbase_address;
    private String hbase_username;
    private String hbase_password;

    //关系数据库配置参数私有变量
    private String rdb_type;
    private String rdb_address;
    private String rdb_username;
    private String rdb_password;
    private ArrayList<String> rdbs_config;  //关系数据库内的各种表信息

    @Override
    public void run() {
        initial_params();
        read_hbase_config();
        rdbs_config = new ArrayList<String>();
        setRdbs_config(read_rdbs_config());

        connectRDB();
    }

    public static void main (String args[]) {
        connectDB t=new connectDB();
        t.run();
    }

    //初始化参数,防止为空时出错
    public void initial_params() {
        setHbase_address("");
        setHbase_password("");
        setHbase_username("");

        setRdb_type("");
        setRdb_address("");
        setRdb_username("");
        setRdb_password("");
    }

    //获取hbase配置文件信息方法
    public void read_hbase_config() {
        try{
            File configFile = new File("file/configs/config.xml");
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document configs = builder.parse(configFile);

            NodeList Hbase = configs.getElementsByTagName("HBASE");
            if (Hbase != null) {
                for(int i = 0; i < Hbase.getLength(); i++) {
                    Element element = (Element) Hbase.item(i);
                    setHbase_address(element.getElementsByTagName("address").item(0).getFirstChild().getNodeValue());
                    setHbase_username(element.getElementsByTagName("username").item(0).getFirstChild().getNodeValue());
                    setHbase_password(element.getElementsByTagName("password").item(0).getFirstChild().getNodeValue());
                }
            } else {
                System.out.print("配置文件中HBASE部分错误,请重新配置.");
                System.exit(0);
            }

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    //获取RDBS配置文件信息方法
    public ArrayList read_rdbs_config() {
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
                    setRdb_type(element.getElementsByTagName("type").item(0).getFirstChild().getNodeValue());
                    setRdb_address(element.getElementsByTagName("address").item(0).getFirstChild().getNodeValue());
                    setRdb_username(element.getElementsByTagName("username").item(0).getFirstChild().getNodeValue());
                    setRdb_password(element.getElementsByTagName("password").item(0).getFirstChild().getNodeValue());
                    NodeList databases = element.getElementsByTagName("database");

                    //遍历所有database
                    for (int j=0; j<databases.getLength(); j++) {

                        Element table = (Element) databases.item(j);
                        String db_name = table.getElementsByTagName("name").item(0).getFirstChild().getNodeValue();

                        //得到当前database下所有的table
                        NodeList table_all = table.getElementsByTagName("table");
                        ArrayList tables = new ArrayList();
                        tables.add("databasename&&"+db_name);
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

    //关系数据库类型判断并跳转到数据库连接
    public void connectRDB() {
        String type=getRdb_type();
        switch(type)
        {
            case "mysql":
                connecttomysql();
                break;
            case "oracle":
                break;
            case "sqlserver":
                break;

        }
    }

    //mysql连接调用
    private void connecttomysql() {
        String db_name = null;//存储数据库名字
        ArrayList<String> tables=new ArrayList<String>();//每个数据库对应的table

        for(int i=0;i<getRdbs_config().size();i++)
        {
            if(getRdbs_config().get(i).startsWith("databasename&&"))
            {
                db_name=getRdbs_config().get(i).substring(14);

                tables=new ArrayList<String>();
                if(db_name.equals("")) {
                    System.out.print("配置文件中DB_name 配置错误,请重新配置.");
                    System.exit(0);
                }
            }
            else
            {
                tables.add(getRdbs_config().get(i));
                if(i+1<getRdbs_config().size())
                {
                    if(getRdbs_config().get(i + 1).startsWith("databasename"));
                    {
                        ConnectMysql connectMysql = new ConnectMysql(db_name, tables, getRdb_type(), getRdb_address(), getRdb_username(), getRdb_password());
                        connectMysql.run();

                        readLogTable readLogTable = new readLogTable(db_name,getRdb_type(), getRdb_address(), getRdb_username(), getRdb_password());
                        readLogTable.run();

                        //hbase参数给予
                        save_hbase save_hbase = new save_hbase(db_name,getRdb_type(), getRdb_address(), getRdb_username(), getRdb_password(),hbase_address,hbase_username,hbase_password);
                        save_hbase.run();
                    }
                }
                else
                {
                    ConnectMysql connectMysql = new ConnectMysql(db_name, tables, getRdb_type(), getRdb_address(), getRdb_username(), getRdb_password());
                    connectMysql.run();

                    readLogTable readLogTable = new readLogTable(db_name,getRdb_type(), getRdb_address(), getRdb_username(), getRdb_password());
                    readLogTable.run();

                    //hbase参数给予
                    save_hbase save_hbase = new save_hbase(db_name,getRdb_type(), getRdb_address(), getRdb_username(), getRdb_password(),hbase_address,hbase_username,hbase_password);
                    save_hbase.run();
                }

            }
        }
    }


    //hbase配置信息私有变量设置  开始
    public String getHbase_address() {
        return hbase_address;
    }
    public void setHbase_address(String hbase_address) {
        this.hbase_address = hbase_address;
    }

    public String getHbase_username() {
        return hbase_username;
    }
    public void setHbase_username(String hbase_username) {
        this.hbase_username = hbase_username;
    }

    public String getHbase_password() {
        return hbase_password;
    }
    public void setHbase_password(String hbase_password) {
        this.hbase_password = hbase_password;
    }
    //hbase配置信息获取  结束


    //RDB配置信息私有变量设置 开始
    public String getRdb_type() {
        return this.rdb_type;
    }
    public void setRdb_type(String rdb_type) {
        this.rdb_type = rdb_type;
    }

    public String getRdb_address() {
        return this.rdb_address;
    }
    public void setRdb_address(String rdb_address) {
        this.rdb_address = rdb_address;
    }

    public String getRdb_username() {
        return this.rdb_username;
    }
    public void setRdb_username(String rdb_username) {
        this.rdb_username = rdb_username;
    }

    public String getRdb_password() {
        return this.rdb_password;
    }
    public void setRdb_password(String rdb_password) {
        this.rdb_password = rdb_password;
    }

    public ArrayList<String> getRdbs_config() {
        return rdbs_config;
    }
    public void setRdbs_config(ArrayList<String> rdbs_config) {
        this.rdbs_config = rdbs_config;
    }
    //RDB配置信息私有变量设置  结束
}
