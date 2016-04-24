import java.lang.String;
import java.io.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import javax.swing.JOptionPane;

public class connectDB {
    public static void main (String args[]) {
        String inputValue = JOptionPane.showInputDialog("Please choose the setting file");
        System.out.print("输入的参数是:"+ inputValue);
        //接收从命令行传输过来的配置文件参数,从而确定应该读取哪一个配置文件
            //差错控制:文件不存在,文件为空,文件中所需参数不完整
        //读取配置文件,获取所有需要的参数
        //根据参数来连接表
            //差错控制:参数不正确导致的表连接失败,
    }
}
