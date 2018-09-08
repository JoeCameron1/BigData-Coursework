package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

public class TransformInputMap extends Mapper<LongWritable, Text, Text, Text> {
    private static Text article_title = new Text(); // Static as a data record spans more than one key value pair
    private static List<String> pages = null;
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        if (!tokenizer.hasMoreTokens()) return;
        String id = tokenizer.nextToken();

         //If line is a revision line, extracts the article title
        if (id.equals("REVISION"))
        {
            if (pages != null)
                for (String s: pages)
                    context.write(article_title, new Text(s));
            tokenizer.nextToken(); // Article title is the third item in the list, so the tokenizer must scroll to account this
            tokenizer.nextToken();
            article_title.set(tokenizer.nextToken());
        }
        // If line is a main line, extracts every proceeding token and writes a key value pair with it and the current article title
        else if (id.equals("MAIN"))
        {
            while (tokenizer.hasMoreTokens())
                context.write(article_title, new Text(tokenizer.nextToken()));
        }
    }
}
