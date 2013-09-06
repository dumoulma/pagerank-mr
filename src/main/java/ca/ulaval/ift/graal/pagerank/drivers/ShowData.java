package ca.ulaval.ift.graal.pagerank.drivers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import ca.ulaval.ift.graal.Util;

public class ShowData {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Util.showSequenceFile(conf, new Path("data/pagerank/depth_49"));
    }
}
