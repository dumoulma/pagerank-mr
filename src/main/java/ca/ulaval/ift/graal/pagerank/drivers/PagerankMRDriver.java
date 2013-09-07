package ca.ulaval.ift.graal.pagerank.drivers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.ulaval.ift.graal.pagerank.mapreduce.PagerankMapper;
import ca.ulaval.ift.graal.pagerank.mapreduce.PagerankReducer;
import ca.ulaval.ift.graal.pagerank.parseurl.mapreduce.ParseUrlMapper;
import ca.ulaval.ift.graal.pagerank.parseurl.mapreduce.ParseUrlReducer;

public class PagerankMRDriver extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(PagerankMRDriver.class);

    private static final String DATA_PATH = "data/parsed_crawl_data.seq";
    private static final float DEFAULT_CONVERGENCE_THRESHOLD = 0.0001f;
    private static final float DEFAULT_DAMPING_FACTOR = 0.85f;
    private static final int MAX_ITERATIONS = 100;
    private static final int INPUT_SIZE = 25;
    private static final int DEFAULT_ROUNDING_PRECISION = 8;

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PagerankMRDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] arg0) throws IOException, ClassNotFoundException, InterruptedException {
        LOG.info("Demo for Pagerank Job -- run()");

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        conf.setFloat("damping.factor", DEFAULT_DAMPING_FACTOR);
        conf.setFloat("convergence.threshold", DEFAULT_CONVERGENCE_THRESHOLD);
        conf.setInt("input.size", INPUT_SIZE);
        conf.setInt("rounding.precision", DEFAULT_ROUNDING_PRECISION);

        Path parsedIntputPath = new Path(DATA_PATH);
        Path parsedOutputPath = new Path("data/pagerank/parsed_output");
        if (fs.exists(parsedOutputPath))
            fs.delete(parsedOutputPath, true);

        Job parseJob = new Job(conf, "Pagerank: Parse URL data job");
        parseJob.setJarByClass(getClass());
        parseCrawlData(parseJob, parsedIntputPath, parsedOutputPath);

        int iterationCount = 1;
        boolean hasConverged = false;
        Path inputPath = parsedOutputPath;
        while (iterationCount < MAX_ITERATIONS && !hasConverged) {
            LOG.info("ITERATION #" + iterationCount);

            Path outputPath = new Path("data/pagerank/depth_" + iterationCount);
            if (fs.exists(outputPath))
                fs.delete(outputPath, true);

            Job pargerankJob = new Job(conf, "Pagerank computation job iteration " + iterationCount);
            pargerankJob.setJarByClass(getClass());
            runPagerankIteration(pargerankJob, inputPath, outputPath);

            long convergenceCounter = pargerankJob.getCounters()
                    .findCounter(PagerankReducer.Counter.CONVERGED).getValue();
            hasConverged = (convergenceCounter < INPUT_SIZE ? false : true);

            iterationCount++;
            fs.delete(inputPath, true);
            inputPath = outputPath;
        }

        return JobStatus.SUCCEEDED;
    }

    private boolean parseCrawlData(Job job, Path input, Path output) throws IOException,
            InterruptedException, ClassNotFoundException {
        job.setMapperClass(ParseUrlMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, input);

        job.setReducerClass(ParseUrlReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true);
    }

    private boolean runPagerankIteration(Job job, Path input, Path output) throws IOException,
            InterruptedException, ClassNotFoundException {
        job.setMapperClass(PagerankMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, input);

        job.setReducerClass(PagerankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true);
    }
}
