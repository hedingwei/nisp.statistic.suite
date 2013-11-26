/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mr.ta.userflow.grep;

import mr.ta.userflow.*;
import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
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
public class Reduce extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        int i=0;
        while(values.hasNext()){
            i++;
            output.collect(new Text(key.toString()+""+i), values.next());
        }
    }

    private Double[] getSumOfOneUnit(List<Long[]> data) {
        double up_tmp = 0.0;
        double down_tmp = 0.0;
        for (Long[] pd : data) {
            up_tmp += pd[0];
            down_tmp += pd[1];
        }
        up_tmp = up_tmp;
        down_tmp = down_tmp;
        return new Double[]{up_tmp, down_tmp};
    }

    public static void main(String[] args) throws ParseException {
        TreeMap<String, Integer> l = new TreeMap<String, Integer>();
        l.put("20131011000001", Integer.SIZE);
        l.put("20131011000000", Integer.SIZE);
        l.put("20131011000005", Integer.SIZE);
        l.put("20131011000002", Integer.SIZE);
        for (String k : l.keySet()) {
            System.out.println(k);
        }
        System.out.println(NumberFormat.getInstance().parse("021").intValue());
    }
}