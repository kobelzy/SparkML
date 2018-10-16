package common;

public class Consts {
	public static final String path = (-1 == System.getProperty("os.name").toLowerCase().indexOf("windows") ? "" : "hadoop/src/main/");
	
	public static final String WEBAPP = path + "webapp/";
	public static final String RESOURCES = path + "resources/";
	public static final String CONFIG = RESOURCES + "config/";
	public static final String LOG4J = RESOURCES + "config/log4j.properties";
	
	//回车
	public static final String ENTER = System.getProperty("line.separator");
	public static final String tab = "\t";//tab

}
