package common.wrapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Properties;

/**
 * version: 1.0.1
 * Created by lzy on 2018/1/4.
 */
public class SmartProperties extends LinkedHashMap<String, String> {

    /**
     * map中key的值，jdbc.properties eg: jdbc.url
     * Param:
     * path: properties文件相对路径，可多次调用load方法
     * Return:
     * Created by lzy on 2018/4/4.
     *
     */
    public void load(String path) {
        String fileName = path.substring(path.lastIndexOf("/") + 1);
        String _fileName = fileName.split("\\.")[0];
        InputStream is = this.getClass().getResourceAsStream(path);
        Properties p = new Properties();
        try {
            p.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (Object k : p.keySet()) {
            String k1 = (String) k;
            Object v = p.get(k);
            String v1 = (String) v;
            //最终可以通过sp.get("文件名.key名")，比如jdbc.url
            put(_fileName + "." + k1, v1);
        }
    }

    public static void main(String[] args) {
        SmartProperties sp = new SmartProperties();
        sp.load("/config/jdbc.properties");
        System.out.println(sp.get("jdbc.url"));
    }

}
