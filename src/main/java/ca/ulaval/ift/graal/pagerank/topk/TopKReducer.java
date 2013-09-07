package ca.ulaval.ift.graal.pagerank.topk;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopKReducer extends Reducer<NullWritable, Text, Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(TopKReducer.class);

    private static final int K = 10;
    private final TreeMap<Double, String> topkMap = new TreeMap<>();

    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            String[] fields = value.toString().split(";");
            Double pagerank = Double.parseDouble(fields[0]);
            String url = fields[1];

            topkMap.put(pagerank, url);

            if (topkMap.size() > K) {
                topkMap.remove(topkMap.firstEntry());
            }
        }

        Map<Double, String> reversed = topkMap.descendingMap();
        Text key2 = new Text();
        Text value = new Text();
        for (Entry<Double, String> entry : reversed.entrySet()) {
            key2.set(entry.getKey().toString());
            value.set(entry.getValue());
            context.write(key2, value);

            LOG.debug("Key: " + key2 + " value: " + value);
        }
    }
}
