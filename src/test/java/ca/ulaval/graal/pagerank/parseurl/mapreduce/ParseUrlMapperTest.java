package ca.ulaval.graal.pagerank.parseurl.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import ca.ulaval.graal.pagerank.parseurl.mapreduce.ParseUrlMapper;

import static org.mockito.Matchers.anyObject;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ParseUrlMapperTest {

    @Mock
    private Mapper<Text, Text, Text, Text>.Context context;

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void givenAUrlWithOneLinkWillEmitOnce() throws IOException, InterruptedException {
        Text key = new Text("http://www.google.com");
        Text value = new Text("http://www.gmail.com");

        ParseUrlMapper mapper = new ParseUrlMapper();
        mapper.map(key, value, context);

        verify(context).write(key, value);
    }

    @Test
    public void givenAUrlWithTwoLinksWillEmitTwice() throws IOException, InterruptedException {
        Text key = new Text("http://www.google.com");
        String outlink1 = "http://www.gmail.com";
        String outlink2 = "http://www.scholar.google.com";
        Text value = new Text(outlink1 + ";" + outlink2);

        ParseUrlMapper mapper = new ParseUrlMapper();
        mapper.map(key, value, context);

        verify(context, times(2)).write((Text) anyObject(), (Text) anyObject());
    }
}
