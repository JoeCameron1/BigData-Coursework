package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class CalculatePageRankReduce extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        StringBuilder outPages = new StringBuilder(" ");
        for (Text value: values)    // Sums each outgoing page using it's PageRank divided by the total number of pages
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            if (!tokenizer.hasMoreTokens()) return;
            String page = tokenizer.nextToken();
            if (!tokenizer.hasMoreTokens()) return;
            Double rank = Double.valueOf(tokenizer.nextToken());
            if (!tokenizer.hasMoreTokens()) return;
            Integer totalPages = Integer.valueOf(tokenizer.nextToken());
            if (totalPages < 1)
                totalPages = 1;
            sum += rank/totalPages;
            outPages.append(page).append(" ");
        }
        double newRank = 0.85*sum+0.15;
        context.write(key, new Text(String.valueOf(newRank)+outPages.toString())); // Writes a key value with the same format to the jobs input to enable iteration
    }
}
