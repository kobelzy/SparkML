package engine;

import org.apache.log4j.PropertyConfigurator;

/**
 * Created by licheng on 2018/1/2.
 */
public class Start {
    public void init(){
        String log4j = Loader.classpath + "/config/log4j.properties";
        PropertyConfigurator.configure(log4j);//加载log4j配置文件
    }
    public static void main(String[] args){

    }
}
