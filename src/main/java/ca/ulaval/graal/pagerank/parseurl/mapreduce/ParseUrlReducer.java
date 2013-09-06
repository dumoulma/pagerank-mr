package ca.ulaval.graal.pagerank.parseurl.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseUrlReducer extends Reducer<Text, Text, Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseUrlReducer.class);

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {
        StringBuilder sb = new StringBuilder("1.0;");
        for (Text value : values) {
            sb.append(value.toString() + ";");
        }
        sb.deleteCharAt(sb.length() - 1);
        Text value2 = new Text(sb.toString());
        context.write(key, value2);

        LOG.debug("Key: " + key.toString() + " value2: " + value2.toString());
    }
}
