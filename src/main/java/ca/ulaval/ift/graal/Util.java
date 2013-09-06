package ca.ulaval.ift.graal;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;

public final class Util {
    public static double round(double unrounded, int precision) {
        BigDecimal bd = new BigDecimal(unrounded);
        BigDecimal rounded = bd.setScale(precision, BigDecimal.ROUND_HALF_EVEN);
        return rounded.doubleValue();
    }

    public static void showSequenceFile(Configuration conf, Path resultPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] outputFileList = fs.listStatus(resultPath);
        for (FileStatus status : outputFileList) {
            if (!status.isDir()) {
                Path path = status.getPath();
                if (!path.getName().equals("_SUCCESS")) {
                    System.out.println("FOUND " + path.toString());
                    try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf)) {
                        Text key = new Text();
                        Text v = new Text();
                        while (reader.next(key, v)) {
                            System.out.println(key + " / " + v);
                        }
                    }
                }
            }
        }
    }

    private Util() {
    }
}

