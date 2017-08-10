package com.mikolaj.app;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.zuinnote.hadoop.bitcoin.format.mapreduce.BitcoinBlockFileInputFormat;

/**
 * Driver
 * Computes total Satoshi output per block
 */
public class BlockTxSumDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generci options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(getConf(), "BlockTxSumDriver program");
        job.setJarByClass(getClass());
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(BlockTxSumMapper.class);
        //job.setReducerClass(BlockTxSumReducer.class);
        //job.setCombinerClass(BlockTxCounterReducer); // if combiner is needed
        job.setNumReduceTasks(0); // for 0 reducer

        job.setInputFormatClass(BitcoinBlockFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + MyUtils.getCurrentDateTime()));

        return job.waitForCompletion(true) ? 0 : -1;
    }
}
