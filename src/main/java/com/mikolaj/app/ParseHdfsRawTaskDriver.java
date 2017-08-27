package com.mikolaj.app;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Parses block, transactions, transaction inputs and transaction outputs to HDFS files - 1 file per task, per type
 */
public class ParseHdfsRawTaskDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generci options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(getConf(), "ParseHdfsRawTask program");
        job.setJarByClass(getClass());
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(ParseHdfsRawTaskMapper.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(org.zuinnote.hadoop.bitcoin.format.mapreduce.BitcoinRawBlockFileInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem.get(getConf()).delete(new Path(args[1]),true); // not sure
        //FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + MyUtils.getCurrentDateTime()));
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // for real HDFS

        return job.waitForCompletion(true) ? 0 : -1;
    }
}
