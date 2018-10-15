package utils;


import common.wrapper.SmartProperties;

/**
 * 加载配置文件
 * version: 1.0.0
 * Created by licheng on 2018/3/30.
 */
public class Loader {
    public static SmartProperties pro;
    public static String classpath;

    static {
        if(classpath == null){
            load();
        }
    }

    private static void load(){
        classpath = Loader.class.getResource("/").getPath();
        System.out.println("classpath:[" + classpath + "]");
        //root = classPath.replaceAll("\\\\", "/") + "WEB-INF/classes/";
        String context = classpath + "config/";
        pro = new SmartProperties();
        for (Enum name : ProNames.values()) {//预加载配置文件
            String properties = context + name.toString() + ".properties";
            pro.load(properties);
        }
    }

    private static enum ProNames {
        jdbc
    }
}
