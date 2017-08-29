package com.mikolaj.app;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Performs SQL / HIVE query:
 * SELECT blk_id, MIN(out_value), MAX(out_value), COUNT(out_value) FROM tx GROUP BY blk_id;
 */
public class MinMaxCountTx extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(TopTx.class);

    public static class MinMaxCountMapper extends Mapper<Object, Text, LongWritable, MinMaxCountTuple> {

        // Our output key and value Writables
        private static LongWritable blkId = new LongWritable(0);
        private static MinMaxCountTuple outTuple = new MinMaxCountTuple();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Parse the input string into a nice map
            String[] txRecord = value.toString().trim().split(",");
            // txRecord[5] -> blk_id
            // txRecord[3] -> out_value

            if (txRecord[5] == null || txRecord[3] == null) {
                // skip this record
                return;
            }

            blkId.set(Long.valueOf(txRecord[5]));
            outTuple.setMin(Long.valueOf(txRecord[3]));
            outTuple.setMax(Long.valueOf(txRecord[3]));
            outTuple.setCount(1);

            context.write(blkId, outTuple);
        }
    }

    public static class MinMaxCountReducer extends Reducer<LongWritable, MinMaxCountTuple, LongWritable, MinMaxCountTuple> {

        private static MinMaxCountTuple result = new MinMaxCountTuple();

        @Override
        public void reduce(LongWritable key, Iterable<MinMaxCountTuple> values, Context context)
                throws IOException, InterruptedException {

            // Initialize our result
            result.setMin(0);
            result.setMax(0);
            int sum = 0;

            // Iterate through all input values for this key
            for (MinMaxCountTuple val : values) {

                // If the value's min is less than the result's min
                // Set the result's min to value's
                if (result.getMin() == 0
                        || val.getMin() < result.getMin()) {
                    result.setMin(val.getMin());
                }

                // If the value's max is less than the result's max
                // Set the result's max to value's
                if (result.getMax() == 0
                        || val.getMax() > result.getMax()) {
                    result.setMax(val.getMax());
                }

                // Add to our sum the count for val
                sum += val.getCount();
            }

            // Set our count to the number of input values
            result.setCount(sum);

            context.write(key, result);
        }
    }

    public MinMaxCountTx() {}

    private static final String INPUT = "input";
    private static final String COMBINER = "useCombiner";
    private static final String OUTPUT = "output";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(new Option(COMBINER, "use combiner"));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (org.apache.commons.cli.ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        boolean useCombiner = cmdline.hasOption(COMBINER);

        LOG.info("Tool name: " + MinMaxCountTx.class.getSimpleName());
        LOG.info(" - input: " + inputPath);
        LOG.info(" - output: " + outputPath);
        LOG.info(" - use combiner: " + useCombiner);

        Configuration conf = getConf();
        conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);

        Job job = Job.getInstance(conf);
        job.setJobName(MinMaxCountTx.class.getName() + ":" + inputPath);
        job.setJarByClass(MinMaxCountTx.class);

        //job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(MinMaxCountTuple.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(MinMaxCountTuple.class);

        job.setMapperClass(MinMaxCountMapper.class);
        job.setReducerClass(MinMaxCountReducer.class);

        // combiner
        if (useCombiner) {
            job.setCombinerClass(MinMaxCountReducer.class);
        }

        // Delete the output directory if it exists already.
        FileSystem.get(conf).delete(new Path(outputPath), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("\nJob Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new MinMaxCountTx(), args);
        System.exit(res);
    }

    // Modified
    // Credits to Adam J. Shook @adamjshook https://github.com/adamjshook/mapreducepatterns
    public static class MinMaxCountTuple implements Writable {
        private long min = 0L;
        private long max = 0L;
        private long count = 0L;

        public long getMin() {
            return min;
        }

        public void setMin(long min) {
            this.min = min;
        }

        public long getMax() {
            return max;
        }

        public void setMax(long max) {
            this.max = max;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            min = in.readLong();
            max = in.readLong();
            count = in.readLong();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(min);
            out.writeLong(max);
            out.writeLong(count);
        }

        @Override
        public String toString() {
            return (min + "\t" + max + "\t" + count);
        }
    }
}
