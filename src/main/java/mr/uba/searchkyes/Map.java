/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mr.uba.searchkyes;

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
            int t = v[4].indexOf("?");
            if(t>0){
                output.collect(new Text(v[1]),  new Text("http://"+v[3]+v[4].substring(0,t)+"$[__]$"+v[5]));
            }else{
                output.collect(new Text(v[1]),  new Text("http://"+v[3]+v[4]+"$[__]$"+v[5]));
            }
            
        } catch (Throwable e) {
            e.printStackTrace();
        }

    }
}