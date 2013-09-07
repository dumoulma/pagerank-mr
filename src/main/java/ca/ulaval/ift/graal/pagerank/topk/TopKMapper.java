package ca.ulaval.ift.graal.pagerank.topk;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map.Entry;

public class TopKMapper extends Mapper<Text, Text, NullWritable, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(TopKMapper.class);
    private static final int K = 10;
    private TreeMap<Double, String> topkMap = new TreeMap<>();

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");
        Double pagerank = Double.parseDouble(fields[0]);
        topkMap.put(pagerank, key.toString());
        if (topkMap.size() > K) {
            topkMap.remove(topkMap.firstEntry().getKey());
        }

        LOG.debug("Pagerank: " + pagerank + " url: " + key);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        NullWritable key = NullWritable.get();
        Text value = new Text();
        for (Entry<Double, String> entry : topkMap.entrySet()) {
            value.set(entry.getKey() + ";" + entry.getValue());
            context.write(key, value);
        }
    }
}
