package ca.ulaval.graal.pagerank.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PagerankMapper extends Mapper<Text, Text, Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(PagerankMapper.class);

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        LOG.info("Key: " + key.toString() + " value: " + value.toString());

        String[] outlinks = value.toString().split(";");
        double pagerank = Double.parseDouble(outlinks[0]);
        int outlinkCount = outlinks.length - 1;
        LOG.info("pagerank: " + pagerank + " outlinkCount: " + outlinkCount);

        Text value2 = new Text();
        Text key2 = new Text();
        for (int i = 1; i < outlinks.length; i++) {
            String outlink = outlinks[i];
            key2.set(outlink + ";" + pagerank);
            value2.set(key.toString() + ";" + pagerank + ";" + outlinkCount);
            context.write(key2, value2);

            LOG.info("Key2: " + key2.toString() + " value2: " + value2.toString());
        }
    }
}
