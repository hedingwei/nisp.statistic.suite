/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mr.ta.userflow;

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

    private String pattern = "20[0-9][0-9][0-9][0-9][0-9][0-9]000[0-9][0-9][0-9]";
    
    public void map(LongWritable key, Text value,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        try {
            String line = value.toString();
            String[] data = line.split(",");
            if (data.length == 6) {
                try {
                    if(!data[4].matches(pattern)){
                        return;
                    }
                    Long.parseLong(data[2]);
                    Long.parseLong(data[3]);
                    Long.parseLong(data[4]);
                    output.collect(new Text(data[0]), new Text(data[2] + "," + data[3]+","+data[4]));
                } catch (Exception e) {
                }

            }

        } catch (Throwable e) {
            e.printStackTrace();
        }

    }
}