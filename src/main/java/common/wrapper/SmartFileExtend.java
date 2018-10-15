package common.wrapper;

import java.io.File;

/**
 * Created by admin on 2016/12/16.
 */
public class SmartFileExtend extends SmartFile {

    public SmartFileExtend(String url) {
        super(url);
    }

    /**
     * 将inputPath文件夹下的所有suffixes的后缀文件合并到outputPath文件
     * Param:
     * inputPath: 待合并的文件或文件夹
     * outputPath： 合并后的文件
     * suffixes: 合并文件的后缀
     * Return:
     * Created by lzy on 2017/12/16.
     */
    public static void merge(String inputPath,String outputPath,String... suffixes){
        File file = new File(inputPath);
        File[] dir = file.listFiles();
        for(File f: dir){
                //如果是路径，就递归到路径中
            if(f.isDirectory()){
                merge(f.getPath(),outputPath,suffixes);
            }else{
                //判断是否是指定的后缀名
                if(!isSuffix(f.getName(),suffixes)){ continue;}
                SmartFileExtend in = new SmartFileExtend(f.getPath());
                StringBuilder content = new StringBuilder(2000);
                while(in.next()){
                    content.append(in.getLine()).append("\n");
                }
                SmartFileExtend out = new SmartFileExtend(outputPath);
                out.addwrite(content.toString());
            }
        }
    }
    
    /**
     * 判断是否包含该后缀名
     * Param: 
     * Return: 
     * Created by lzy on 2017/12/16.
     */
    private static boolean isSuffix(String fileName,String... suffixes){
        String[] name = fileName.split("\\.");
        if(name.length != 2){
            return false;
        }
        String suffix = name[1];
        for(String s: suffixes){
            if(s.equals(suffix)){
                return true;
            }
        }
        return false;
    }
    
    public static void main(String[] args){
        merge("D:\\workspace\\hna","D:\\temp\\result.txt",new String[]{"java","scala"});
    }
}
