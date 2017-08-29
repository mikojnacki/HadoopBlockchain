package com.mikolaj.app;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

import java.io.IOException;
import java.util.Arrays;

/**
 * Performs SQL / HIVE query:
 * SELECT address, SUM(value) FROM txout GROUP BY address;
 */
public class SumAddress extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(TopTx.class);

    public static class SumMapper extends Mapper<Object, Text, Text, LongWritable> {

        private static Text address = new Text();
        private static LongWritable outValue = new LongWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] txoutRecord = value.toString().trim().split(",");
            address.set(txoutRecord[2]);
            outValue.set(Long.valueOf(txoutRecord[3]));
            context.write(address, outValue);
        }
    }

    public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private static LongWritable sumValue = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            sumValue.set(0);
            for (LongWritable value : values) {
                sumValue.set(sumValue.get() + value.get());
            }
            context.write(key, sumValue);
        }
    }

    public SumAddress() {}

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
        } catch (ParseException exp) {
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

        LOG.info("Tool name: " + SumAddress.class.getSimpleName());
        LOG.info(" - input: " + inputPath);
        LOG.info(" - output: " + outputPath);
        LOG.info(" - use combiner: " + useCombiner);

        Configuration conf = getConf();
        conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);

        Job job = Job.getInstance(conf);
        job.setJobName(SumAddress.class.getName() + ":" + inputPath);
        job.setJarByClass(SumAddress.class);

        //job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReducer.class);

        // combiner
        if (useCombiner) {
            job.setCombinerClass(SumReducer.class);
        }

        // Delete the output directory if it exists already.
        FileSystem.get(conf).delete(new Path(outputPath), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("\nJob Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        // Save execution time on disk
        MyUtils.generateReport(SumAddress.class.getSimpleName(), MyUtils.getCurrentDateTime(),System.currentTimeMillis() - startTime);

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new SumAddress(), args);
        System.exit(res);
    }
}
