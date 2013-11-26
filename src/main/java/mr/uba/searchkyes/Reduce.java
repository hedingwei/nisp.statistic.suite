/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mr.uba.searchkyes;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
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
        try {

            TreeMap<String, Integer> keywords = new TreeMap<String, Integer>();
            TreeMap<String, Integer> urls = new TreeMap<String, Integer>();

            HashSet<String> set = new HashSet<String>();
            StringBuilder sb = new StringBuilder();
            String tmp = null;
            int p = 0;
            String kw = null;
            String ul = null;
            int t = -1;
            while (values.hasNext()) {
                tmp = values.next().toString();
                p = tmp.indexOf("$[__]$");
                kw = tmp.substring(0, p);
                ul = tmp.substring(p + "$[__]$".length(), tmp.length());
//                t = ul.indexOf("?");
//                if (t > 0) {
//                    ul = ul.substring(0, t);
//                }
                if (keywords.containsKey(kw)) {
                    keywords.put(kw, keywords.get(kw) + 1);
                } else {
                    keywords.put(kw, 1);
                }

                if (urls.containsKey(ul)) {
                    urls.put(ul, urls.get(ul) + 1);
                } else {
                    urls.put(ul, 1);
                }
            }

            StringBuilder rst = new StringBuilder();
            rst.append("{\"keywords:\":{");

            for (String k : keywords.keySet()) {
                rst.append("\"").append(k).append("\":").append(keywords.get(k)).append(",");
            }
            if (!keywords.keySet().isEmpty()) {
                rst.deleteCharAt(rst.length() - 1);
            }
            rst.append("},\"urls\":{");

            for (String u : urls.keySet()) {
                rst.append("\"").append(u).append("\":").append(urls.get(u)).append(",");
            }
            if (!urls.keySet().isEmpty()) {
                rst.deleteCharAt(rst.length() - 1);
            }
            rst.append("}}");

            output.collect(new Text(key), new Text(rst.toString()));
        } catch (Throwable e) {
            e.printStackTrace();
        }

    }
}
