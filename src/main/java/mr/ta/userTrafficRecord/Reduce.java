/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mr.ta.userTrafficRecord;

import java.io.IOException;
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
        StringBuilder result = new StringBuilder();
        while (values.hasNext()) {
            result.append(values.next().toString()).append(";");
        }
        output.collect(key, new Text(result.toString()));
    }
}