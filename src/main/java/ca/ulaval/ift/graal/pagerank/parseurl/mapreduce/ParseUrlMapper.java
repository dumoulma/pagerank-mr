package ca.ulaval.ift.graal.pagerank.parseurl.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseUrlMapper extends Mapper<Text, Text, Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseUrlMapper.class);

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] outlinks = value.toString().split(";");
        Text value2 = new Text();
        for (String outlink : outlinks) {
            value2.set(outlink);
            context.write(key, value2);

            LOG.debug("Key2: " + key.toString() + " value2: " + value2.toString());
        }
    }
}
