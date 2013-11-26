/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mr.sd.qq;

import HanNanWordKey.filterusers.*;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

    
    
    public void map(LongWritable key, Text value,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
            
        try {
            String line = value.toString();
            if (!line.startsWith("qqid")) {
                return;
            }
            String[] v = line.split(",");
            String qq = v[3].substring(v[3].indexOf("|") + 1);
            output.collect(new Text(v[1]), new Text(qq));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}