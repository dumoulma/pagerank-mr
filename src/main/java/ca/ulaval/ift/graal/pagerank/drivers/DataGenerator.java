package ca.ulaval.ift.graal.pagerank.drivers;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class DataGenerator extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);

    private static final String INTPUT_VOCAB_FILENAME = "data/vocab/english_words.txt";
    private static final String OUTPUT_SEQ_FILENAME = "data/parsed_crawl_data.seq";
    private static final int MAX_URLS = 25;
    private static final int MAX_OUTLINKS = 5;
    private static final int MAX_WORDS_IN_URL = 2;
    private static Random random = new Random();

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DataGenerator(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] arg0) throws IOException {
        final List<String> domains = Lists.newArrayList(".net", ".com", ".edu");
        final List<String> words = new ArrayList<>();
        List<String> urls = new ArrayList<>();

        int wordCount = 0;
        try (Scanner wordScanner = new Scanner(new FileReader(new File(INTPUT_VOCAB_FILENAME)))) {
            wordScanner.useDelimiter("\\W");
            while (wordScanner.hasNext()) {
                String nextWord = wordScanner.next().trim().toLowerCase();
                if (nextWord.length() > 3) {
                    ++wordCount;
                    words.add(nextWord);

                    LOG.debug(wordCount + ": " + nextWord);
                }
            }
        }
        LOG.info("Vocab words read: " + wordCount);

        urls = generateUrls(words, domains);

        int urlWrittenCount = 0;
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        Path dataPath = new Path(OUTPUT_SEQ_FILENAME);
        if (fs.exists(dataPath))
            fs.delete(dataPath, true);

        try (SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, dataPath, Text.class,
                Text.class)) {
            urlWrittenCount = writeUrlsToSequenceFile(writer, urls);
        }

        LOG.info("Successfully wrote " + urlWrittenCount + " urls");
        return JobStatus.SUCCEEDED;
    }

    private List<String> generateUrls(List<String> words, List<String> domains) {
        List<String> urls = new ArrayList<>();
        for (int i = 0; i < MAX_URLS; i++) {
            int nWords = random.nextInt(MAX_WORDS_IN_URL) + 1;
            StringBuilder url = new StringBuilder("http://www");
            for (int j = 0; j < nWords; j++) {
                int wordIndex = random.nextInt(words.size());
                url.append("." + words.get(wordIndex));
            }
            url.append(domains.get(random.nextInt(domains.size())));
            if (url.indexOf("..") != -1) {
                LOG.warn("Malformed URL: " + url.toString());
            }
            urls.add(url.toString());
        }
        return urls;
    }

    private int writeUrlsToSequenceFile(SequenceFile.Writer writer, List<String> urls)
            throws IOException {
        int urlWrittenCount = 0;

        Text key = new Text();
        Text value = new Text();
        for (String url : urls) {
            key.set(url);
            int nOutLinks = random.nextInt(MAX_OUTLINKS);
            if (nOutLinks == 0)
                nOutLinks += random.nextInt(MAX_OUTLINKS);
            
            List<String> outlinks = new ArrayList<>();
            for (int i = 0; i < nOutLinks; i++) {
                int outLinkIndex = random.nextInt(urls.size());
                String outlink = urls.get(outLinkIndex);
                while (outlink.equals(url))
                    outlink = urls.get(random.nextInt(urls.size()));
                if (!outlinks.contains(outlink)) {
                    outlinks.add(outlink);
                }
            }
            StringBuilder sb = new StringBuilder();
            for (String outlink : outlinks) {
                sb.append(outlink + ";");
            }
            if (sb.length() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }

            value.set(sb.toString());
            writer.append(key, value);

            urlWrittenCount++;
            LOG.info("key: " + key.toString() + " value: " + value.toString());
        }

        return urlWrittenCount;
    }

}
