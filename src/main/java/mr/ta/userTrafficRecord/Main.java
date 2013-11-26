package mr.ta.userTrafficRecord;

import HanNanWordKey.GetFileList;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 *
 * Usage: java -cp .:*:dependency/* mr.ta.userflow.Main [namenodeip] [date]
 * [userlistFile] Format: namenodeip: 192.168.100.13 startdate: 2013-10-16
 * userlistFile: /tmp/userlist.txt Consequence: All user infomation will be
 * generated in a set of files located at /home/tmp/userflow/userflow. And
 * filterd file called output.txt will be placed at the program's working space
 * dir.
 *
 * @author hedingwei
 */
public class Main {

    public static void main(String[] args) throws IOException {
//        cleanDir("/home/tmp/userflow");
        GetFileList gList = new GetFileList(args[0]);
        List<String> fileList = gList.getResultList("/nisp/statistic/usertraffic_raw", args[1], args[2]);
        String[] fl = new String[fileList.size()];
        for (int i = 0; i < fileList.size(); i++) {
            fl[i] = fileList.get(i);
        }
        executeMapReduce(args[0], fl, args[1]+"_"+args[2]);
//        grep(args[2]);
//        String s = "986281176,165462128,1151743304,527976.3733333333,68756.74666666667,15486.96,3404.9866666666667,,1440,2013-10-01";
//        System.out.println(s.split(",")[7]);
    }

    public static void executeMapReduce(String namenodeip, String[] srcFolder, String date) throws IOException {
        /**
         * JobConf：map/reduce的job配置类，向hadoop框架描述map-reduce执行的工作
         * 构造方法：JobConf()、JobConf(Class exampleClass)、JobConf(Configuration
         * conf)等
         */
        String dstFolder = "/nisp/statistic/usertraffic_mapped/" + date;
        Configuration config = new Configuration();
        config.set("fs.default.name", "hdfs://" + namenodeip + ":9000"); // HDFS配置
        config.set("mapred.job.tracker", "maprfs://" + namenodeip + ":9001"); // MapReduce配置
//        FileSystem fs = FileSystem.get(config);

        JobConf conf = new JobConf(config);
        // JobConf conf=new JobConf(WordCount.class);
        conf.setJobName("mr.ta.userTrafficRecord"); // 设置一个用户定义的job名称
        conf.setOutputKeyClass(Text.class); // 为job的输出数据设置Key类
        conf.setOutputValueClass(Text.class); // 为job输出设置value类
        conf.setMapperClass(Map.class); // 为job设置Mapper类
//        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class); // 为job设置Reduce类
        conf.setJarByClass(Main.class);
//        conf.setNumReduceTasks(100);
        conf.setInputFormat(TextInputFormat.class); // 为map-reduce任务设置InputFormat实现类
        conf.setOutputFormat(TextOutputFormat.class); // 为map-reduce任务设置OutputFormat实现类
        /**
         * InputFormat描述map-reduce中对job的输入定义 setInputPaths():为map-reduce
         * job设置路径数组作为输入列表 setInputPath()：为map-reduce job设置路径数组作为输出列表
         */
        Path[] paths = new Path[srcFolder.length];
        for (int i = 0; i < srcFolder.length; i++) {
            paths[i] = new Path(srcFolder[i]);
        }
        FileInputFormat.setInputPaths(conf, paths);
        FileOutputFormat.setOutputPath(conf, new Path(dstFolder));
        RunningJob job = JobClient.runJob(conf); // 运行一个job
//        job.waitForCompletion();

        System.out.println("Job Done Fine");
    }
//    public static void cleanDir(String dir) {
//        File f = new File(dir);
//        if (!f.exists()) {
//            return;
//        }
//        if (f.list().length == 0) {
//            f.delete();
//        } else {
//            for (File tf : f.listFiles()) {
//                if (tf.isFile()) {
//                    tf.delete();
//                } else {
//                    cleanDir(tf.getAbsolutePath());
//                }
//            }
//        }
//        f.delete();
//    }
//
//    public static void grep(String userListFile) throws FileNotFoundException, IOException {
//        BufferedReader br = new BufferedReader(new FileReader(userListFile));
//        HashSet<String> users = new HashSet<String>();
//        String tmp = null;
//        String ptr = null;
//        while ((tmp = br.readLine()) != null) {
//            ptr = tmp.trim();
//            if (!ptr.equals("")) {
//                users.add(ptr);
//            }
//        }
//        br.close();
//        System.out.println("filter size:" + users.size());
//        PrintWriter pw = new PrintWriter(new FileOutputStream("output.txt"));
//        String[] d = null;
//        NumberFormat nf = NumberFormat.getPercentInstance();
//
//        int totalFiles = new File("/home/tmp/userflow").listFiles().length;
//        int i = 0;
//        for (File f : new File("/home/tmp/userflow").listFiles()) {
//            i++;
//            if (!f.getName().startsWith("part-")) {
//                continue;
//            }
//            if (!f.isFile()) {
//                continue;
//            }
//
//            br = new BufferedReader(new FileReader(f));
//            while ((tmp = br.readLine()) != null) {
//                d = tmp.split("\t");
////                System.out.println("check : "+d +"\t"+users.contains(d));
//                if (users.contains(d[0])) {
//                    pw.println(tmp);
//                }
//            }
//            br.close();
//            System.out.println(nf.format(i / (totalFiles + 0.0)));
//        }
//        pw.close();
//    }
}
