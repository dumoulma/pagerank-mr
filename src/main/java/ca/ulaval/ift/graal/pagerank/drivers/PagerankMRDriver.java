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
    private static final int MAX_ITERATIONS = 50;

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PagerankMRDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] arg0) throws IOException, ClassNotFoundException, InterruptedException {
        LOG.info("Demo for Pagerank Job -- run()");

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        conf.setFloat("damping.factor", 0.85f);
        conf.setFloat("error.threshold", 0.01f);
        Path parsedIntputPath = new Path(DATA_PATH);
        Path parsedOutputPath = new Path("data/pagerank/initial");
        parseCrawlData(conf, "Pagerank: Parse URL data job", parsedIntputPath, parsedOutputPath);

        int iteration = 1;
        boolean hasConverged = false;
        Path inputPath = parsedOutputPath;
        while (iteration < MAX_ITERATIONS && !hasConverged) {
            LOG.info("ITERATION #" + iteration);

            conf.setInt("current.iteration", iteration);

            Path outputPath = new Path("data/pagerank/depth_" + iteration);
            if (fs.exists(outputPath))
                fs.delete(outputPath, true);
            String jobname = "Pagerank computation job iteration " + iteration;
            hasConverged = runPagerankIteration(conf, jobname, inputPath, outputPath);

            iteration++;
            inputPath = outputPath;
        }

        return JobStatus.SUCCEEDED;
    }

    private boolean parseCrawlData(Configuration conf, String jobName, Path input, Path output)
            throws IOException, InterruptedException, ClassNotFoundException {
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output))
            fs.delete(output, true);

        Job job = new Job(conf, jobName);
        job.setJarByClass(getClass());

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

    private boolean runPagerankIteration(Configuration conf, String jobName, Path input, Path output)
            throws IOException, InterruptedException, ClassNotFoundException {
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output))
            fs.delete(output, true);

        Job job = new Job(conf, jobName);
        job.setJarByClass(getClass());

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

        boolean completed = job.waitForCompletion(true);
        if (!completed)
            throw new IOException("job has not completed successfully!");

        long convergenceCounter = job.getCounters().findCounter(PagerankReducer.Counter.CONVERGED)
                .getValue();
        LOG.info("PagerankReducer Counter: " + convergenceCounter);

        return convergenceCounter >= 100 ? true : false;
    }
}
