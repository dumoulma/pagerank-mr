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
    private static final double DEFAULT_CONVERGENCE_THRESHOLD = 0.001;
    private double convergence_threshold;
    private int N = 1;
    private static int DEFAULT_ROUNDING_PRECISION = 5;
    private int roundingPrecision;

    public static enum Counter {
        CONVERGED,
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();

        dampingFactor = conf.getFloat("damping.factor", (float) DEFAULT_DAMPING_FACTOR);
        convergence_threshold = conf.getFloat("convergence.threshold",
                (float) DEFAULT_CONVERGENCE_THRESHOLD);
        N = conf.getInt("input.size", N);
        roundingPrecision = conf.getInt("rounding.precision", DEFAULT_ROUNDING_PRECISION);
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {
        String[] keyFields = key.toString().split(";");
        String url = keyFields[0];
        double oldPagerank = Double.parseDouble(keyFields[1]);

        LOG.debug("key: " + key);
        double pagerank = 0.0;
        StringBuilder outlinks = new StringBuilder();
        for (Text value : values) {
            String[] fields = value.toString().split(";");
            outlinks.append(fields[0] + ";");
            pagerank += Double.parseDouble(fields[1]) / Integer.parseInt(fields[2]);
        }
        pagerank *= dampingFactor/N;
        pagerank += (1 - dampingFactor)/N;
        pagerank = Util.round(pagerank, roundingPrecision);

        if (isConverged(oldPagerank, pagerank)) {
            context.getCounter(Counter.CONVERGED).increment(1);
            LOG.debug("CONVERGED! " + url + " pagerank is: " + pagerank);
        }
        double pagerankChange = Util.round(pagerank - oldPagerank, 5);
        LOG.debug("url: " + url + " pagerank: " + pagerank + " change: " + pagerankChange);

        key.set(url);
        context.write(key, new Text(pagerank + ";" + outlinks.toString()));
    }

    private boolean isConverged(double oldValue, double newValue) {
        return Math.abs(oldValue - newValue) < convergence_threshold;
    }
}
