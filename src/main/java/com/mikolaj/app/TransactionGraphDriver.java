package com.mikolaj.app;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.zuinnote.hadoop.bitcoin.format.mapreduce.BitcoinBlockFileInputFormat;
import org.zuinnote.hadoop.bitcoin.format.mapreduce.BitcoinRawBlockFileInputFormat;

/**
 * Driver
 * Computes total Satoshi output per Bitcoin address
 */
public class TransactionGraphDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generci options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(getConf(), "TransactionGraph program");
        job.setJarByClass(getClass());
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setMapperClass(TransactionGraphMapper.class);
        //job.setReducerClass(AddressOutputReducer.class); // the same Reducer for raw input
        //job.setCombinerClass(AddressOutputReducer); // if combiner is needed
        job.setNumReduceTasks(0); // for 0 reducer

        job.setInputFormatClass(BitcoinRawBlockFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + MyUtils.getCurrentDateTime()));

        return job.waitForCompletion(true) ? 0 : -1;
    }
}
