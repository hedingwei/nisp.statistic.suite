package HanNanWordKey;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import HDFSTools.HDFSOperation;
import java.text.SimpleDateFormat;

public class GetFileList {

    HDFSOperation hdfs;

    public GetFileList(String ip) {
        hdfs = new HDFSOperation(ip); //HDFS操作对象
    }

    /**
     * 获取文件的所有列表
     *
     * @param dstFolder
     * @throws IOException
     */
    public List<String> getList(String dstFolder, String startData, String endData) throws IOException {
        List<String> fileList = hdfs.listFileDir(dstFolder);  //获取到时间那一级别的目录
        List<String> temporaryFileLIst = new ArrayList<String>(); //存放最终的文件列表 
        List<String> finalFileLIst = new ArrayList<String>();//最终的结果文件
        for (int i = 0; i < fileList.size(); i++) {
            String currentData = fileList.get(i).substring(fileList.get(i).lastIndexOf("/") + 1);
            if (startData.compareTo(currentData) <= 0 && endData.compareTo(currentData) >= 0) {
                temporaryFileLIst.add(fileList.get(i));
            }
        }
        System.out.println("temporaryFileList:");
        System.out.println(temporaryFileLIst);
        for (int i = 0; i < temporaryFileLIst.size(); i++) {
            finalFileLIst.addAll(hdfs.listDir(temporaryFileLIst.get(i)));
        }
        System.out.println("finalFileLIst:");
        System.out.println(finalFileLIst.size());
        System.out.println(finalFileLIst);
        return finalFileLIst;
    }

    public List<String> getResultList(String dstFolder, String startData, String endData) throws IOException {
        List<String> fileList = hdfs.listFileDir(dstFolder);  //获取到时间那一级别的目录
        List<String> temporaryFileLIst = new ArrayList<String>(); //存放最终的文件列表 
        List<String> finalFileLIst = new ArrayList<String>();//最终的结果文件
        for (int i = 0; i < fileList.size(); i++) {
            String currentData = fileList.get(i).substring(fileList.get(i).lastIndexOf("/") + 1);
            if (startData.compareTo(currentData) <= 0 && endData.compareTo(currentData) >= 0) {
                temporaryFileLIst.add(fileList.get(i));
            }
        }
        System.out.println("temporaryFileList:");
        System.out.println(temporaryFileLIst);
        for (int i = 0; i < temporaryFileLIst.size(); i++) {
            for(String p:hdfs.listFiles(temporaryFileLIst.get(i))){
                if(!p.contains("_SUCCESS")){
                    finalFileLIst.add(p);
                }
            }
//            finalFileLIst.addAll(hdfs.listFiles(temporaryFileLIst.get(i)));
        }
        System.out.println("finalFileLIst:");
        System.out.println(finalFileLIst.size());
//        System.out.println(finalFileLIst);
        return finalFileLIst;
    }

    public List<String> getListRecurcive(String dstFolder, String startData, String endData) throws IOException {
        List<String> fileList = hdfs.listFileDir(dstFolder);  //获取到时间那一级别的目录
        List<String> temporaryFileLIst = new ArrayList<String>(); //存放最终的文件列表 
        List<String> finalFileLIst = new ArrayList<String>();//最终的结果文件
        for (int i = 0; i < fileList.size(); i++) {
            String currentData = fileList.get(i).substring(fileList.get(i).lastIndexOf("/") + 1);
            if (startData.compareTo(currentData) <= 0 && endData.compareTo(currentData) >= 0) {
                temporaryFileLIst.add(fileList.get(i));
            }
        }
        System.out.println("temporaryFileList:");
        System.out.println(temporaryFileLIst);
        for (int i = 0; i < temporaryFileLIst.size(); i++) {
            finalFileLIst.addAll(hdfs.listFileDirRecurcive(temporaryFileLIst.get(i)));
        }
        System.out.println("finalFileLIst:");
        System.out.println(finalFileLIst.size());
        System.out.println(finalFileLIst);
        return finalFileLIst;
    }

    /**
     * 读取用户账号文件
     *
     * @throws IOException
     */
    public List<String> readUserFile(String userFilePath) throws IOException {
        BufferedReader read = new BufferedReader(new FileReader(new File(userFilePath)));
        List<String> userId = new ArrayList<String>();
        for (String line = null; (line = read.readLine()) != null;) {
            userId.add(line);
        }
        return userId;
    }
}
