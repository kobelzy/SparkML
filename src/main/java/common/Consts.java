package common;

public class Consts {
	public static final String path = (-1 == Tools.getOSName().indexOf("windows") ? "" : "hadoop/src/main/");
	
	public static final String WEBAPP = path + "webapp/";
	public static final String RESOURCES = path + "resources/";
	public static final String WEBDEFAULT = RESOURCES + "jetty/webdefault.xml";
	public static final String CONFIG = RESOURCES + "config/";
	public static final String MODULE_PATH = "com.smart.modules";
	public static final String LOG4J = RESOURCES + "config/log4j.properties";
	
	public static final String ENTER = System.getProperty("line.separator");//回车
	public static final String tab = "\t";//tab

}
