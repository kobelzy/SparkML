package engine;


import common.wrapper.SmartProperties;

/**
 * 加载配置文件
 * version: 1.0.1
 * Created by lzy on 2018/3/30.
 */
public class Loader {
    public static SmartProperties pro;
    public static String classpath;

    //只会在第一次调用的时候执行一次，并且全局唯一，所以之后调用pro.get不会重新执行读取配置文件。
    static {
        if(classpath == null){
            load();
        }
    }

    private static void load(){
        pro = new SmartProperties();
        classpath = Loader.class.getResource("/").getPath();
        System.out.println("classpath:[" + classpath + "]");
            //预加载配置文件
        for (Enum name : ProNames.values()) {
            String propertiesPath = "/config/" + name.toString() + ".properties";
            System.out.println("load file:[" + propertiesPath + "]");
            pro.load(propertiesPath);
        }
    }

    /**
    需要加载的配置文件，只需要写文件名
     jdbc,hadoop,spark,log4j,oozie
     */
    private enum ProNames {
        jdbc
    }
    public static void main(String[] args) {
    System.out.println(Loader.pro.get("jdbc.url"));

}
    }