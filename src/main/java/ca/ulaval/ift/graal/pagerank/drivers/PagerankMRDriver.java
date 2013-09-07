package ca.ulaval.ift.graal.pagerank.drivers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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

import ca.ulaval.ift.graal.Util;
import ca.ulaval.ift.graal.pagerank.mapreduce.PagerankMapper;
import ca.ulaval.ift.graal.pagerank.mapreduce.PagerankReducer;
import ca.ulaval.ift.graal.pagerank.parseurl.mapreduce.ParseUrlMapper;
import ca.ulaval.ift.graal.pagerank.parseurl.mapreduce.ParseUrlReducer;
import ca.ulaval.ift.graal.pagerank.topk.TopKMapper;
import ca.ulaval.ift.graal.pagerank.topk.TopKReducer;

public class PagerankMRDriver extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(PagerankMRDriver.class);

    private static final String DATA_PATH = "data/pagerank/parsed_crawl_data.seq";
    private static final float DEFAULT_DAMPING_FACTOR = 0.85f;
    private static final int MAX_ITERATIONS = 50;
    private static final int INPUT_SIZE = 100;
    private static final int DEFAULT_ROUNDING_PRECISION = 8;

    // TODO figure out how to check for convergence
    // private static final float DEFAULT_CONVERGENCE_THRESHOLD = 0.0001f;

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
        conf.setInt("input.size", INPUT_SIZE);
        conf.setInt("rounding.precision", DEFAULT_ROUNDING_PRECISION);

        Path parsedIntputPath = new Path(DATA_PATH);
        Path parsedOutputPath = new Path("data/pagerank/parsed_output");
        if (fs.exists(parsedOutputPath))
            fs.delete(parsedOutputPath, true);

        Job parseJob = new Job(conf, "Pagerank: Parse URL data job");
        parseJob.setJarByClass(getClass());
        runParseUrlJob(parseJob, parsedIntputPath, parsedOutputPath);

        int iterationCount = 1;
        Path inputPath = parsedOutputPath;
        while (iterationCount < MAX_ITERATIONS) {
            LOG.info("ITERATION #" + iterationCount);

            Path outputPath = new Path("data/pagerank/depth_" + iterationCount);
            if (fs.exists(outputPath))
                fs.delete(outputPath, true);

            Job pargerankJob = new Job(conf, "Pagerank computation job iteration " + iterationCount);
            pargerankJob.setJarByClass(getClass());
            runPagerankIterationJob(pargerankJob, inputPath, outputPath);

            iterationCount++;
            fs.delete(inputPath, true);
            inputPath = outputPath;
        }

        Path topkOutputPath = new Path("data/pagerank/topk");
        if (fs.exists(topkOutputPath)) {
            fs.delete(topkOutputPath, true);
        }
        Job topkJob = new Job(conf, "Pagerank topK job");
        topkJob.setJarByClass(getClass());
        runTopKJob(topkJob, inputPath, topkOutputPath);

        fs.delete(inputPath, true);

        Util.showSequenceFile(conf, topkOutputPath);

        return JobStatus.SUCCEEDED;
    }

    private boolean runParseUrlJob(Job job, Path input, Path output) throws IOException,
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

    private boolean runPagerankIterationJob(Job job, Path input, Path output) throws IOException,
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

    private boolean runTopKJob(Job job, Path input, Path output) throws IOException,
            InterruptedException, ClassNotFoundException {
        job.setMapperClass(TopKMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, input);

        job.setReducerClass(TopKReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true);
    }
}
