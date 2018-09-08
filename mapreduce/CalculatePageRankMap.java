package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;



public class CalculatePageRankMap extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        if (!tokenizer.hasMoreTokens()) return;
        String fromPage = tokenizer.nextToken();
        if (!tokenizer.hasMoreTokens()) return;

        // If the lines is not structured as expected, ignore the line
        try {
            Float score = Float.valueOf(tokenizer.nextToken());
            int count = tokenizer.countTokens()-2; // Minus the score and main page
            if (count < 1)
                count = 1;
            while (tokenizer.hasMoreTokens())
                context.write(new Text(tokenizer.nextToken()), new Text(fromPage + " " + score + " " + count)); // Writes key value pair with article title as key,
        } catch (NumberFormatException e) {
            return;
        }


    }
}
