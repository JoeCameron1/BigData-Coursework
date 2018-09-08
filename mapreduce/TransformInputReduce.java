package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TransformInputReduce extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder val = new StringBuilder("1.0 "); // Writes a starting PageRank value of 1.0
        for (Text t: values) // Appends all of the outgoing pages into one line after the starting PageRank
            val.append(t.toString()).append(" ");
        context.write(key, new Text(val.toString()));
    }
}
