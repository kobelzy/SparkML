package common;

import org.apache.log4j.Logger;

import java.util.UUID;

public class Tools {
	public Logger logger  =  Logger.getLogger(getClass());
	public static String getUUID(){
		return UUID.randomUUID().toString();
	}

	/**
	 * 获取操作系统名称
	 * @return
     */
	public static String getOSName(){
		return System.getProperty("os.name").toLowerCase();
	}


	public static void main(String[] args){
	}

}
