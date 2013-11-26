/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package HanNanWordKey.filterusers;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author Administrator
 */
public class Map extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        try {
            String line = value.toString();
            String[] v = line.split("\t");
            if (v[5].equals("-")) {
                return;
            }
            output.collect(new Text(v[1]), new Text(v[5]));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}