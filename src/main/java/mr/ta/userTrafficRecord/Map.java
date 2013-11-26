/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mr.ta.userTrafficRecord;

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
            String[] data = line.split("\t");
            output.collect(new Text(data[0]), new Text(data[1]));
        } catch (Throwable e) {
//            e.printStackTrace();
        }

    }
}