package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PageRank extends Configured implements Tool {
    
    public int run(String[] args) throws Exception {
        int iterations;
        if (args.length > 2)
        {
            try {
                iterations = Integer.parseInt(args[2]);
                if (iterations < 1)
                    iterations = 1;
            } catch (NumberFormatException e) {
                System.out.println("Usage: PageRank <inputfile> <outputfile> [iterations]");
                iterations = 1;
            }
        }
        else
            iterations = 1;
        //------------------------------------------------
        // Job 1 - Transform Input Info
        
        Job transformInputJob = Job.getInstance(getConf(), "PageRank");
        transformInputJob.setJarByClass(PageRank.class);
        
        transformInputJob.setMapperClass(TransformInputMap.class);
        transformInputJob.setReducerClass(TransformInputReduce.class);
        
        transformInputJob.setInputFormatClass(TextInputFormat.class);
        transformInputJob.setOutputKeyClass(Text.class);
        transformInputJob.setOutputValueClass(Text.class);
        transformInputJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(transformInputJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(transformInputJob, new Path("tmp/" + args[1] + "_stage1"));
        
        transformInputJob.waitForCompletion(true);
        
        //------------------------------------------------
        // Job 2 - Calculate PageRank Score
        Job calculatePageRankJob;
        for (int i = 0; i < iterations; i++)
        {
            calculatePageRankJob = Job.getInstance(getConf(), "PageRank");
            calculatePageRankJob.setJarByClass(PageRank.class);

            calculatePageRankJob.setMapperClass(CalculatePageRankMap.class);
            calculatePageRankJob.setReducerClass(CalculatePageRankReduce.class);

            calculatePageRankJob.setInputFormatClass(TextInputFormat.class);
            calculatePageRankJob.setOutputKeyClass(Text.class);
            calculatePageRankJob.setOutputValueClass(Text.class);
            calculatePageRankJob.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.setInputPaths(calculatePageRankJob, new Path("tmp/" + args[1] + "_stage" + (i+1)));
            FileOutputFormat.setOutputPath(calculatePageRankJob, new Path("tmp/" + args[1] + "_stage" + (i+2)));

            calculatePageRankJob.waitForCompletion(true);
        }
        
        //------------------------------------------------
        // Job 3 - Produce appropriate output
        
        Job produceOutputJob = Job.getInstance(getConf(), "PageRank");
        produceOutputJob.setJarByClass(PageRank.class);

        produceOutputJob.setMapperClass(ProduceOutputMap.class);
        produceOutputJob.setReducerClass(ProduceOutputReduce.class);

        produceOutputJob.setInputFormatClass(TextInputFormat.class);
        produceOutputJob.setOutputKeyClass(Text.class);
        produceOutputJob.setOutputValueClass(Text.class);
        produceOutputJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(produceOutputJob, new Path("tmp/" + args[1] + "_stage" + (iterations+1)));
        FileOutputFormat.setOutputPath(produceOutputJob, new Path(args[1]));
        
        return produceOutputJob.waitForCompletion(true) ? 0 : 1;
        
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
    }
    
}
