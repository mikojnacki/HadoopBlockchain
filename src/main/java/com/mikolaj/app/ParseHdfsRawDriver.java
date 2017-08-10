package com.mikolaj.app;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.zuinnote.hadoop.bitcoin.format.mapreduce.BitcoinRawBlockFileInputFormat;

/**
 * Created by Mikolaj on 03.08.17.
 */
public class ParseHdfsRawDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generci options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(getConf(), "ParseHdfsRaw program");
        job.setJarByClass(getClass());
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(ParseHdfsRawMapper.class);
        //job.setReducerClass(AddressOutputReducer.class); // the same Reducer for raw input
        //job.setCombinerClass(AddressOutputReducer); // if combiner is needed
        job.setNumReduceTasks(0); // for 0 reducer

        job.setInputFormatClass(org.zuinnote.hadoop.bitcoin.format.mapreduce.BitcoinRawBlockFileInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem.get(getConf()).delete(new Path(args[1]),true); // not sure
        //FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + MyUtils.getCurrentDateTime()));
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // for real HDFS

        return job.waitForCompletion(true) ? 0 : -1;
    }
}
