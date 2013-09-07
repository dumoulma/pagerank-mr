package ca.ulaval.ift.graal.pagerank.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.ulaval.ift.graal.Util;

public class PagerankReducer extends Reducer<Text, Text, Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(PagerankReducer.class);

    private static final double DEFAULT_DAMPING_FACTOR = 0.85;
    private double dampingFactor;
    // private static final double DEFAULT_CONVERGENCE_THRESHOLD = 0.001;
    // private double convergence_threshold;
    private int N = 1;
    private static int DEFAULT_ROUNDING_PRECISION = 5;
    private int roundingPrecision;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();

        dampingFactor = conf.getFloat("damping.factor", (float) DEFAULT_DAMPING_FACTOR);
        N = conf.getInt("input.size", N);
        roundingPrecision = conf.getInt("rounding.precision", DEFAULT_ROUNDING_PRECISION);
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {
        String url = key.toString();

        LOG.debug("PROCESSING URL: " + url);

        double pagerank = 0.0;
        StringBuilder outlinks = new StringBuilder();
        for (Text value : values) {
            LOG.debug("PROCESSING: " + value);

            String[] fields = value.toString().split(";");
            outlinks.append(fields[0] + ";");
            pagerank += Double.parseDouble(fields[1]) / Integer.parseInt(fields[2]);
        }
        pagerank *= dampingFactor;
        pagerank += (1 - dampingFactor) / N;
        pagerank = Util.round(pagerank, roundingPrecision);

        key.set(url);
        Text value = new Text(pagerank + ";" + outlinks);
        context.write(key, value);

        LOG.debug("OUTPUT: key: " + key + " Value: " + value);
    }
}
