package common.wrapper;

import java.io.*;
import java.nio.file.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static java.nio.file.StandardWatchEventKinds.*;

/**
 * 对文本文件的基本操作
 * 2018年7月8日
 * v1.2.5
 *
 * @author lzy
 */
public class SmartFile {
    private String inputPath, outputPath;
    private FileInputStream fis;
    private InputStreamReader read;
    private BufferedReader br;
    private String line;
    private File inputFile;
    private File outputFile;

    public SmartFile(String inputPath) {
        this.inputPath = inputPath;
        this.inputFile = new File(inputPath);
    }

    public SmartFile(File inputFile) {
        this.inputPath = inputFile.getPath();
        this.inputFile = inputFile;
    }

    public SmartFile(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.inputFile = new File(inputPath);
        this.outputPath = outputPath;
        this.outputFile = new File(outputPath);
    }

    public SmartFile(File inputFile, File outputFile) {
        this.inputFile = inputFile;
        this.inputPath = inputFile.getPath();
        this.outputFile = outputFile;
        this.outputPath = outputFile.getPath();
    }

    public long length() {
        return inputFile.length();
    }

    public String name() {
        return inputFile.getName();
    }

    public String path() {
        return inputFile.getParent();
    }

    public File mkdir() {
        return mkdir(inputPath);
    }

    private File mkdir(File file) {
        if (!file.exists()) {
            file.mkdirs();
        }
        return file;
    }

    private File mkdir(String filePath) {
        File file = new File(filePath);
        return mkdir(file);
    }

    public boolean next() {
        try {
            if (fis == null) {
                fis = new FileInputStream(new File(inputPath));//读取到内存
                read = new InputStreamReader(fis);//解读内存数据
                br = new BufferedReader(read);
            }
            if ((line = br.readLine()) != null) {
                return true;
            } else {
                br.close();
                read.close();
                fis.close();
            }
        } catch (IOException e) {
            e.printStackTrace();

        }
        return false;
    }

    public String getLine() {
        return line;
    }

    /**
     * 重写
     * Created by lzy on 2018年8月10日.
     */
    public void rewrite(String str) {
        write(str, false);
    }

    /**
     * 追加字符串
     * Created by licheng on 2018年8月10日.
     */
    public void addwrite(String str) {
        write(str, true);
    }

    /**
     * 追加一个文件的所有内容
     * Param:
     * Return:
     * Created by lzy on 2018/9/29.
     */
    public void addwrite(File file) {
        String separator = System.getProperty("line.separator");
        if (this.length() != 0) {
            //如果文件有内容，就写入一个回车
            write(separator, true);
        }
        SmartFile sf = new SmartFile(file.getPath());
        while (sf.next()) {
            write(sf.getLine() + separator, true);
        }
    }

    /**
     * append true:追加	false:重写
     * Created by lzy on 2018年8月10日.
     */
    private void write(String str, boolean append) {
        File path = new File(path());
        if (!path.exists()) {
            //如果不存在
            path.mkdirs();//创建该目录
        }
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(new File(inputPath), append));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                bw.write(str);
                bw.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * position: 插入的位置
     * data: 插入的数据
     * Created by lzy on 2018年8月17日.
     */
    public void insert(long position, String data) {
        try {
            RandomAccessFile raf = new RandomAccessFile(inputPath, "rw");
            raf.seek(position);
            //计算出从光标开始至结束的长度
            int length = new Long(raf.length() - position).intValue();
            byte[] buff = new byte[length];
            raf.read(buff);
            raf.seek(position);
            //插入数据
            raf.write(data.getBytes());
            //追加文件插入点之后的内容
            raf.write(buff);
            raf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除文件或文件夹
     * 该文件夹也一并删除
     * Param:
     * Return:
     * Created by lzy on 2018/7/11.
     */
    public void delete() {
        File file = new File(inputPath);
        //递归
        delete0(file);
    }

    private boolean delete0(File file) {
        if (file.isDirectory()) {
            String[] children = file.list();
            for (String child : children) {
                boolean res = delete0(new File(file, child));
                if (!res) {
                    return false;
                }
            }
        }
        return file.delete();
    }

    /**
     * copy单个文件
     * Param:
     * inputFile: 一个文件
     * outputFile: 一个目录
     * Return:
     * Created by lzy on 2017/11/27.
     */
    private void copyFile(File inputFile, File outputFile, boolean isDel) {
        mkdir(outputFile);
        String outputFilePath = outputFile.getPath() + "/" + inputFile.getName();
        outputFile = new File(outputFilePath);
        InputStream is = null;
        OutputStream os = null;
        try {
            is = new FileInputStream(inputFile);
            os = new FileOutputStream(outputFile);
            byte[] b = new byte[is.available()];
            int n = 0;
            while ((n = is.read(b)) > 0) {
                os.write(b, 0, n);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                is.close();
                os.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (isDel) inputFile.delete();
    }

    /**
     * copy一个目录中的所有文件到另一个目录
     * Param:
     * inputFile: 一个目录
     * outputFile: 一个目录
     * Return:
     * Created by lzy on 2017/11/27.
     */
    private void copyAll(File inputFile, File outputFile, boolean isDel) {
        File[] files = inputFile.listFiles();
        for (File f : files) {
            copyFile(f, outputFile, isDel);
        }
    }

    /**
     * Param:
     * isDel: 是否删除被copy的文件
     * Return:
     * Created by licheng on 2017/11/28.
     */
    public void copy(boolean isDel) {
        //是目录
        if (inputFile.isDirectory()) {
            copyAll(inputFile, outputFile, isDel);
        } else {
            copyFile(inputFile, outputFile, isDel);
        }
    }

    public void copy() {
        //是目录
        if (inputFile.isDirectory()) {
            copyAll(inputFile, outputFile, false);
        } else {
            copyFile(inputFile, outputFile, false);
        }
    }

    /**
     * 目录文件监控
     * Param:
     * wi : 监控接口
     * Return:
     * Created by lzy on 2017/11/23.
     */
    public void watcher(final WatcherInterface wi) {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                watcher0(wi);
            }
        });
        thread.start();
    }

    private void watcher0(WatcherInterface wi) {
        WatchService watcher = null;
        try {
            watcher = FileSystems.getDefault().newWatchService();
            Path path = Paths.get(inputPath);
            path.register(watcher, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);
            while (true) {
                WatchKey key = watcher.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind kind = event.kind();
                        //事件可能lost or discarded
                    if (kind == OVERFLOW) {
                        continue;
                    }
                    WatchEvent<Path> e = (WatchEvent<Path>) event;
                    String fileName = e.context().toString();
                    String inputPath = this.inputPath.endsWith("/") ? this.inputPath : this.inputPath + "/";
                    File file = new File(inputPath + fileName);
                    switch (kind.name()) {
                        case "ENTRY_CREATE":
                            wi.onCreate(file);
                            break;
                        case "ENTRY_DELETE":
                            wi.onDelete(file);
                            break;
                        default:
                    }
                }
                if (!key.reset()) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 压缩文件或文件夹
     * Param:
     * Return:
     * Created by lzy on 2018/7/14.
     */
    public void zip(String zipPath) {
        zip(new File(inputPath));
    }

    public void zip(File file) {
        File[] files = file.listFiles();
        for (File f : files) {
            if (f.isDirectory()) {
                System.out.println("dir:" + f.getName());
                zip(f);
            } else {
                System.out.println("file:" + f.getName());
            }
        }
    }

    /**
     * 解压文件夹内的所有压缩文件
     * Param:
     * inputPath: 需要解压的目录
     * outputPath: 解压后存放的目录
     * Return:
     * Created by lzy on 2018/7/25.
     */
    public void unzip() {
        unzip(inputPath);
    }

    /**
     * 递归解压所有目录
     * Param:
     * Return:
     * Created by lzy on 2018/7/25.
     */
    private void unzip(String path) {
        File[] files = new File(path).listFiles();
        for (File f : files) {
            if (f.isDirectory()) {
                unzip(f.getPath());
            } else {
                unzipFile(f.getPath());
            }
        }
    }

    /**
     * 解压单个文件
     * Param:
     * Return:
     * Created by lzy on 2018/7/25.
     */
    private void unzipFile(String path) {
        File source = new File(path);
        if (source.exists()) {
            ZipInputStream zis = null;
            BufferedOutputStream bos = null;
            try {
                zis = new ZipInputStream(new FileInputStream(source));
                ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null && !entry.isDirectory()) {
                    //File target = new File(source.getParent(), entry.getName());//输出到当前目录
                    String sourcePath = source.getParent().replaceAll("\\\\", "/");
                    sourcePath = sourcePath.endsWith("/") ? sourcePath : sourcePath + "/";
                    //保留原有目录结构
                    String location = outputPath + sourcePath.substring(inputPath.length());
                    File target = new File(location, entry.getName());
                    if (!target.getParentFile().exists()) {
                        target.getParentFile().mkdirs();
                    }
                    bos = new BufferedOutputStream(new FileOutputStream(target));
                    int read;
                    byte[] buffer = new byte[1024 * 10];
                    while ((read = zis.read(buffer, 0, buffer.length)) != -1) {
                        bos.write(buffer, 0, read);
                    }
                    bos.flush();
                }
                zis.closeEntry();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    zis.close();
                    bos.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public interface WatcherInterface {
        void onCreate(File file);

        void onDelete(File file);
    }

    /**
     * 调用demo
     * lzy
     * 2018年7月8日
     */
    public static void main(String[] args) {
		/*SmartFile sf = new SmartFile("D:/temp/EUTRANCELL3.csv");
		SmartFile sf2 = new SmartFile("D:/temp/EUTRANCELL4.csv");
		while(sf.next()){
			String line = sf.getLine().replaceAll("\"","");
			sf2.addwrite(line + "\n");
		}*/

        //SmartFile sf = new SmartFile("D://temp/aaa/");
        //sf.copy(true);
        SmartFile sf = new SmartFile("D:/temp/1/");
        sf.watcher(new WatcherInterface() {

            @Override
            public void onCreate(File file) {
                System.out.println("nimabi");
            }

            @Override
            public void onDelete(File file) {

            }
        });
        System.out.println("caonima");
    }

    public static final String path = "D:\\temp\\1\\";
}
