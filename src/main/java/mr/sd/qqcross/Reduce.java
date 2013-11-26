/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mr.sd.qqcross;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author Administrator
 */
public class Reduce extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        try {
            HashSet<String> set = new HashSet<String>();
            
            StringBuilder sb = new StringBuilder();
            while (values.hasNext()) {
                set.add(values.next().toString());
                
            }
            for(String s: set){
                sb.append(s).append(" ");
            }
            output.collect(new Text(key), new Text(sb.toString()));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}