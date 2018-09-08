package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProduceOutputReduce extends Reducer<Text, Text, Text, Text> {
    private List<Pair> items = new ArrayList<>();

    public void reduce(Text key, Iterable<Text> values, Context context) {
        // For each key (PageRank score), adds all corresponding pages to a list, keeping a descending order
        for (Text value: values)
            sortedAdd(items, Float.valueOf(key.toString()), value.toString());
    }

    public void cleanup(Context context) throws IOException, InterruptedException
    {
        // Outputs each key value in the form <Article Title> <PageRank score> in descending order
        for (Pair p: items)
        {
            context.write(new Text(p.page), new Text(String.valueOf(p.score)));
        }
    }

    /**
     * Adds key value pair of score and page to items in a descending order based on score
     * @param items list of pairs
     * @param score score of current pair
     * @param page article title of current pair
     */
    private void sortedAdd(List<Pair> items, Float score, String page)
    {
        if (items.isEmpty())
            items.add(new Pair(score, page));
        else {
            int i = 0;
            while (items.get(i).score > score) i++;
            if (i > items.size())
                items.add(new Pair(score, page));
            else
                items.add(i, new Pair(score,page));
        }

    }

    /**
     * Additional class to allow for items to retain their article title after sorting
     */
    private class Pair
    {
        float score;
        String page;
        Pair(float score, String page)
        {
            this.score = score;
            this.page = page;
        }
    }
}
