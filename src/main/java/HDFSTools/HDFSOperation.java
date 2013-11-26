package HDFSTools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class HDFSOperation {

    Configuration config;
    List<String> filelist = new ArrayList<String>(); //获取文件的所有列表

    public HDFSOperation(String ip) {
        config = new Configuration();
        config.set("fs.default.name", "hdfs://"+ip+":9000");//初始化HDFS
    }

    /*
     * 上传本地文件到文件系统HDFS
     */
    public void uploadLocalFile2HDFS(String s, String d) throws IOException {
        FileSystem hdfs = FileSystem.get(config);
        Path src = new Path(s);
        Path dst = new Path(d);
        hdfs.copyFromLocalFile(src, dst);
        hdfs.close();
    }

    /**
     * 获取当前目录下的文件夹列表
     *
     * @param dir
     * @throws IOException
     */
    public List<String> listFileDir(String dir) throws IOException {
        List<String> dateFilePathlist = new ArrayList<String>();
        FileSystem fs = FileSystem.get(config);
        Path path = new Path(dir);
        if (fs.exists(path)) {
            FileStatus[] stats = fs.listStatus(new Path(dir));
            for (int i = 0; i < stats.length; ++i) {
                if (stats[i].isDir()) {
                    dateFilePathlist.add(stats[i].getPath().toString());
                }
            }
        }
        return dateFilePathlist;
    }
    
    public List<String> listFiles(String dir) throws IOException {
        List<String> dateFilePathlist = new ArrayList<String>();
        FileSystem fs = FileSystem.get(config);
        Path path = new Path(dir);
        if (fs.exists(path)) {
            FileStatus[] stats = fs.listStatus(new Path(dir));
            for (int i = 0; i < stats.length; ++i) {
                if (!stats[i].isDir()) {
                    dateFilePathlist.add(stats[i].getPath().toString());
                }
            }
        }
        return dateFilePathlist;
    }

    public List<String> listFileDirRecurcive(String dir) throws IOException {
        List<String> dateFilePathlist = new ArrayList<String>();
        final FileSystem fs = FileSystem.get(config);
        Path path = new Path(dir);
        if (fs.exists(path)) {
            FileStatus[] stats = fs.listStatus(new Path(dir), new PathFilter() {
                public boolean accept(Path path) {
                    try {
                        return !fs.isFile(path);
                    } catch (IOException ex) {
                        Logger.getLogger(HDFSOperation.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    return false;
                }
            });

            if (stats.length == 0) {
                dateFilePathlist.add(path.toString());
                return dateFilePathlist;
            } else {
                for (FileStatus fileStatus : stats) {
                    dateFilePathlist.add(fileStatus.getPath().toString());
                }
            }
        }
        return dateFilePathlist;
    }

    /*
     * 创建新文件，并写入
     */
    public void createNewHDFSFile(String toCreateFilePath, String content)
            throws IOException {
        FileSystem hdfs = FileSystem.get(config);
        FSDataOutputStream os = hdfs.create(new Path(toCreateFilePath));
        os.write(content.getBytes("UTF-8")); //使用UTF-8编码格式
        os.close();
        hdfs.close();
    }

    /*
     * 删除文件
     */
    public boolean deleteHDFSFile(String dst) throws IOException {
        FileSystem hdfs = FileSystem.get(config);
        Path path = new Path(dst);
        boolean isDeleted = hdfs.delete(path);
        hdfs.close();
        return isDeleted;
    }

    /**
     * 读取文件
     */
    public byte[] readHDFSFile(String dst) throws Exception {
        FileSystem fs = FileSystem.get(config);
        // check if the file exists
        Path path = new Path(dst);
        if (fs.exists(path)) {
            FSDataInputStream is = fs.open(path);
            // get the file info to create the buffer
            FileStatus stat = fs.getFileStatus(path);//Return a file status object that represents the path.读取在HDFS的文件状态
            // create the buffer
            byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat
                    .getLen()))];
            is.readFully(0, buffer);
            is.close();
            fs.close();

            return buffer;
        } else {
            throw new Exception("the file is not found .");
        }
    }

    /**
     * 目录操作
     */
    public void mkdir(String dir) throws IOException {
        FileSystem fs = FileSystem.get(config);
        fs.mkdirs(new Path(dir));
        fs.close();
    }

    /**
     * 删除目录
     */
    public void deleteDir(String dir) throws IOException {
        FileSystem fs = FileSystem.get(config);
        fs.delete(new Path(dir));
        fs.close();
    }

    /**
     * 获取文件列表,循环遍历文件获得这个文件夹下面所有的文件列表
     *
     * @param dir
     * @throws IOException
     */
    public List<String> listAll(String dir) throws IOException {
        List<String> rst = new ArrayList<String>();
        FileSystem fs = FileSystem.get(config);
        Path path = new Path(dir);
        if (fs.exists(path)) {
            FileStatus[] stats = fs.listStatus(new Path(dir));
            for (int i = 0; i < stats.length; ++i) {
                if (stats[i].isDir()) {
                    rst.addAll(listAll(stats[i].getPath().toString()));
                } else {
                    // is s symlink in linux
                    rst.add(stats[i].getPath().toString());
                }
            }
        }
        fs.close();
        return rst;
    }

    public List<String> listDir(String dir) throws IOException {
        List<String> rst = new ArrayList<String>();
        final FileSystem fs = FileSystem.get(config);
        Path path = new Path(dir);
        if (fs.exists(path)) {
            FileStatus[] stats = fs.listStatus(new Path(dir), new PathFilter() {
                public boolean accept(Path path) {
                    try {
                        return !fs.isFile(path);
                    } catch (IOException ex) {
                        Logger.getLogger(HDFSOperation.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    return false;
                }
            });
            if (stats.length == 0) {
                rst.add(dir);
            } else {
                for (int i = 0; i < stats.length; ++i) {
                    if (stats[i].isDir()) {
                        if (fs.listStatus(stats[i].getPath(), new PathFilter() {
                            public boolean accept(Path path) {
                                try {
                                    return !fs.isFile(path);
                                } catch (IOException ex) {
                                    Logger.getLogger(HDFSOperation.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                return false;
                            }
                        }).length == 0) {
                            rst.add(stats[i].getPath().toString());
                        } else {
                            rst.addAll(listDir(stats[i].getPath().toString()));
                        }
                    } else {
                    }
                }
            }

        }
        fs.close();
        return rst;
    }

    public static void main(String[] args) throws Exception {
//        HDFSOperation hdfs = new HDFSOperation();
//        long a = System.currentTimeMillis();
//        hdfs.uploadLocalFile2HDFS("F://ipRecord2.txt",
//                "hdfs://10.8.4.170:9000/liangdelin/ipRecord2.txt");
//        System.out.println("上传时间：" + (System.currentTimeMillis() - a));
        //这里的路径问题，如果针对于IP地址后面不加"/"则数据会存储在HDFs的user/Administrator/下  例如liangdelin/Hello.txt
//		hdfs.createNewHDFSFile("/liangdelin32/555/data1.txt", "datadtaakjsdlasddnfglsnlnglngfl");
    }
}
