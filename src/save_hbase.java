/**
 * Created by huguantao on 16/5/21.
 */
public class save_hbase extends Thread{
    public String hbase_address;
    public String hbase_username;
    public String hbase_password;

    public save_hbase(String hbase_address,String hbase_username,String hbase_password) {
        this.hbase_address = hbase_address;
        this.hbase_username = hbase_username;
        this.hbase_password = hbase_password;
    }

    public void main() {
//        System.out.println(hbase_address);
//        System.out.println(hbase_username);
//        System.out.println(hbase_password);

    }
}
