/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mr.ta.userflow.grep;

import mr.ta.userflow.*;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author hedingwei
 */
public class Map extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, Text> {

    private JobConf conf = null;
    private String id = ".*089865353318.*";
    
    @Override
    public void configure(JobConf job) {
        super.configure(job); //To change body of generated methods, choose Tools | Templates.
        this.conf = job;
        this.id = job.get("user.id");
//        this.id = ".*089865353318.*";
    }

    
    
    public void map(LongWritable key, Text value,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        try {
            String line = value.toString();
            if(line.matches(this.id)){
                output.collect(new Text(this.id), value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}