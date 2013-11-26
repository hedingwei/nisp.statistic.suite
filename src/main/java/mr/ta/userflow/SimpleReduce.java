/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mr.ta.userflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author Administrator
 */
public class SimpleReduce extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> {

    private String pattern = "20[0-9][0-9][0-9][0-9][0-9][0-9]000[0-9][0-9][0-9]";

    public void reduce(Text k2, Iterator<Text> itrtr, OutputCollector<Text, Text> oc, Reporter rprtr) throws IOException {
        String[] tmp = null;
        Long[] l = null;
        String time = null;
        TreeMap<String, List<Long[]>> data = new TreeMap<String, List<Long[]>>();

        while (itrtr.hasNext()) {
            tmp = itrtr.next().toString().split(",");
            l = new Long[2];
            l[0] = Long.parseLong(tmp[0]);
            l[1] = Long.parseLong(tmp[1]);
            time = tmp[2];
            if (!time.matches(pattern)) {
                continue;
            }
            if (data.containsKey(time)) {
                data.get(time).add(l);
            } else {
                List<Long[]> d = new ArrayList<Long[]>();
                d.add(l);
                data.put(time, d);
            }

        }
        for (String k : data.keySet()) {
            oc.collect(k2, new Text(k+":"+data.get(k)));
        }
    }
}
