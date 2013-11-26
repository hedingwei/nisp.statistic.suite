package mr.sd.qqcross;

import mr.sd.qq.*;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Main {

    public static void main(String[] args) throws IOException {
        GetFileList gList = new GetFileList("");
        List<String> fileList = gList.getList("/mapreduce/ShareDetect/src/24h", args[1], args[2]);
        String[] fl = new String[fileList.size()];
        for (int i = 0; i < fileList.size(); i++) {
            fl[i] = fileList.get(i);
        }
        executeMapReduce(fl, args[0]);
    }

    public static void executeMapReduce(String[] srcFolder, String dstFolder) throws IOException {

        /**
         * JobConf：map/reduce的job配置类，向hadoop框架描述map-reduce执行的工作
         * 构造方法：JobConf()、JobConf(Class exampleClass)、JobConf(Configuration
         * conf)等
         */
        Configuration config = new Configuration();
        config.set("fs.default.name", "hdfs://192.168.100.13:9000"); // HDFS配置
        config.set("mapred.job.tracker", "maprfs://192.168.100.13:9001"); // MapReduce配置
        JobConf conf = new JobConf(config);
        // JobConf conf=new JobConf(WordCount.class);
        conf.setJobName("mr.sd.qq.sd"); // 设置一个用户定义的job名称

        conf.setOutputKeyClass(Text.class); // 为job的输出数据设置Key类
        conf.setOutputValueClass(Text.class); // 为job输出设置value类

        conf.setMapperClass(Map.class); // 为job设置Mapper类

        conf.setReducerClass(Reduce.class); // 为job设置Reduce类
        conf.setJarByClass(Main.class);
        conf.setNumReduceTasks(100);
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

        JobClient.runJob(conf); // 运行一个job
    }
}
