package com.mikolaj.app;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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
import java.util.*;

/**
 * Performs SQL / HIVE query:
 * SELECT hash, out_value FROM tx ORDER BY out_value DESC LIMIT arg;
 */
public class TopTx extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(TopTx.class);

    public static class TopTxMapper extends Mapper<Object, Text, NullWritable, PairOfLongString> {

        // Stores a map of outAddresses to value of Satoshis
        private TreeMap<LongWritable, Text> outAddressSatoshiMap = new TreeMap<LongWritable, Text>();

        private PairOfLongString mapOutput = new PairOfLongString();
        int k;

        @Override
        public void setup(Context context) {
            k = context.getConfiguration().getInt("n", 1000);
        }

        @Override
        public void map(Object key, Text input, Context context) throws IOException, InterruptedException {

            String[] inputString = input.toString().split(",");

            // sorted on values (becomes TreeMap keys) in ascending order
            outAddressSatoshiMap.put(new LongWritable(Long.valueOf(inputString[3])), new Text(inputString[1]));
            if (outAddressSatoshiMap.size() > k) {
                outAddressSatoshiMap.remove(outAddressSatoshiMap.firstKey());
            }
            LOG.info("size: " + outAddressSatoshiMap.size());
            LOG.info("n: " + k);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // emitting
            for (Map.Entry<LongWritable, Text> element : outAddressSatoshiMap.entrySet()) {
                mapOutput.set(element.getKey().get(), element.getValue().toString());
                context.write(NullWritable.get(), mapOutput);
            }

        }
    }

    public static class TopTxReducer extends Reducer<NullWritable, PairOfLongString, Text, LongWritable> {

        // Stores a map of outAddresses to value of Satoshis
        private TreeMap<LongWritable, Text> outAddressSatoshiMap = new TreeMap<LongWritable, Text>();
        Text outAddress = new Text();
        LongWritable satoshi = new LongWritable(0);
        int k;

        @Override
        public void setup(Context context) {
            k = context.getConfiguration().getInt("n", 1000);
        }

        @Override
        public void reduce(NullWritable nullKey, Iterable<PairOfLongString> pairs, Context context)
                throws IOException, InterruptedException {

            for (PairOfLongString p : pairs) {
                outAddressSatoshiMap.put(new LongWritable(p.getKey()), new Text(p.getValue()));
                if (outAddressSatoshiMap.size() > k) {
                        outAddressSatoshiMap.remove(outAddressSatoshiMap.firstKey());
                }
            }

            // emitting
//            for (Map.Entry<LongWritable, Text> element : outAddressSatoshiMap.entrySet()) {
//                outAddress.set(element.getValue().toString());
//                satoshi.set(element.getKey().get());
//                context.write(outAddress, satoshi);
//            }

            // emitting in reversed order
            TreeMap<LongWritable, Text> reversed = new TreeMap<LongWritable, Text>(Collections.reverseOrder());
            reversed.putAll(outAddressSatoshiMap);
            for (Map.Entry<LongWritable, Text> element : reversed.entrySet()) {
                outAddress.set(element.getValue().toString());
                satoshi.set(element.getKey().get());
                context.write(outAddress, satoshi);
            }
        }
    }

    public TopTx() {}

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String TOP = "top";

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
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("top n").create(TOP));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int n = Integer.parseInt(cmdline.getOptionValue(TOP));

        LOG.info("Tool name: " + TopTx.class.getSimpleName());
        LOG.info(" - input: " + inputPath);
        LOG.info(" - output: " + outputPath);
        LOG.info(" - top: " + n);

        Configuration conf = getConf();
        conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
        conf.setInt("n", n);

        Job job = Job.getInstance(conf);
        job.setJobName(TopTx.class.getName() + ":" + inputPath);
        job.setJarByClass(TopTx.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(PairOfLongString.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(TopTx.TopTxMapper.class);
        job.setReducerClass(TopTx.TopTxReducer.class);

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
        int res = ToolRunner.run(new TopTx(), args);
        System.exit(res);
    }
}
