/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mr.ta.userflow;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
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

    private String pattern = "20[0-9][0-9][0-9][0-9][0-9][0-9]000[0-9][0-9][0-9]";
    private NumberFormat nf = NumberFormat.getNumberInstance();
    private String thisDate = "2013-01-01";

    @Override
    public void configure(JobConf job) {
        super.configure(job); //To change body of generated methods, choose Tools | Templates.
        thisDate = job.get("thisDate");
    }

    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        try {
            String busyTimePattern = ".*2[45678][0-9]";
            TreeMap<String, List<Long[]>> data = new TreeMap<String, List<Long[]>>();
//            TreeMap<String, List<Long[]>> allPeaks = new TreeMap<String, List<Long[]>>();
//            TreeMap<String, Double[]> graph = new TreeMap<String, Double[]>();
            long day_total = 0;
            long day_up_total = 0;
            long day_down_total = 0;
            double busy_up_peak_speed = 0;
            double busy_down_peak_speed = 0;
            double up_tmp = 0;
            double down_tmp = 0;
            String time = null;
            String[] tmp = null;
            Long[] l = null;

            /**
             * pharse : A 1. collect all up and down traffic of a time unit, and
             * map them by timeunit. 2. sums the up and down traffic of whole
             * day.
             */
            while (values.hasNext()) {
                tmp = values.next().toString().split(",");
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
                day_up_total = day_up_total + l[0];
                day_down_total = day_down_total + l[1];
            }
            day_total = day_up_total + day_down_total;
            /**
             * End : A
             */
            Double[] speed = null;
            busy_up_peak_speed = 0;
            busy_down_peak_speed = 0;
            double busy_up_day_speed = 0;
            double busy_down_day_speed = 0;

            String busy_up_day_time = "";
            String busy_down_day_time = "";

            for (String timeUnit : data.keySet()) {

                speed = getSumOfOneUnit(data.get(timeUnit));
                up_tmp = speed[0];
                down_tmp = speed[1];
//                graph.put(timeUnit, speed);

                if (up_tmp > busy_up_day_speed) {
                    busy_up_day_speed = up_tmp;
                    busy_up_day_time = timeUnit;
                }
                if (down_tmp > busy_down_day_speed) {
                    busy_down_day_speed = down_tmp;
                    busy_down_day_time = timeUnit;
                }

                if (timeUnit.matches(busyTimePattern)) {
                    if (up_tmp > busy_up_peak_speed) {
                        busy_up_peak_speed = up_tmp;
                    }
                    if (down_tmp > busy_down_peak_speed) {
                        busy_down_peak_speed = down_tmp;
                    }
                }
            }

            boolean error = ((day_up_total == 0 || day_down_total == 0) && (day_total != 0)) || ((busy_down_day_speed == 0 || busy_up_day_speed == 0) && (busy_down_day_speed + busy_up_day_speed) != 0) || ((busy_down_peak_speed == 0 || busy_up_peak_speed == 0) && (busy_up_peak_speed + busy_down_peak_speed) != 0);
            if (!error) {
                output.collect(key, new Text(day_up_total * 8 + "," + day_down_total * 8 + "," + day_total * 8 + "," + (busy_up_day_speed / 300.0) * 8 + "," + (busy_down_day_speed / 300.0) * 8 + "," + (busy_up_peak_speed / 300.0) * 8 + "," + (busy_down_peak_speed / 300.0) * 8 + "," + "," + data.keySet().size() * 5 + "," + this.thisDate));
            }
        } catch (Exception e) {
            e.printStackTrace();
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