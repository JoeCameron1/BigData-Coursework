package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class ProduceOutputMap extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        if (!tokenizer.hasMoreTokens()) return;
        String page = tokenizer.nextToken();
        Float score = Float.valueOf(tokenizer.nextToken());
        context.write(new Text(String.valueOf(score)), new Text(page));
        // Generates a key value pair with score as the key and the page title as value
    }
}
