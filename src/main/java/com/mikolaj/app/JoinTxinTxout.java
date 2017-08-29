package com.mikolaj.app;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Performs SQL / HIVE query:
 * SELECT txin.prev_out, txin.prev_out_index, txout.address, txout.value FROM txin JOIN txout ON txin.tx_id = txout.tx_id;
 */
public class JoinTxinTxout extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(TopTx.class);

    public static class TxinJoinMapper extends Mapper<Object, Text, Text, Text> {
        // txin JOIN txout ON txin.tx_id = txout.tx_id

        private static Text outkey = new Text();
        private static Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Split the input string
            String[] txinRecord = value.toString().trim().split(",");

            if (txinRecord[4] == null) {
                return;
            }

            // The foreign join key is the txinPrevOut
            outkey.set(txinRecord[4]);

            // Flag this record for the reducer and then output
            outvalue.set("A" + txinRecord[2] + " " + txinRecord[3]);
            context.write(outkey, outvalue);
        }
    }

    public static class TxoutJoinMapper extends Mapper<Object, Text, Text, Text> {
        // txin JOIN txout ON txin.tx_id = txout.tx_id

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Split the input string
            String[] txoutRecord = value.toString().trim().split(",");

            if (txoutRecord[5] == null) {
                return;
            }

            // Set foreign key
            outkey.set(txoutRecord[5]);

            // Flag this record for the reducer and then output
            outvalue.set("B" + txoutRecord[2] + " " + txoutRecord[3]);
            context.write(outkey, outvalue);
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        // txin JOIN txout ON txin.tx_id = txout.tx_id

        private ArrayList<Text> listA = new ArrayList<Text>();
        private ArrayList<Text> listB = new ArrayList<Text>();
        private String joinType = null;

        @Override
        public void setup(Context context) {
            // Get the type of join from our configuration
            joinType = context.getConfiguration().get("join.type");
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Clear our lists
            listA.clear();
            listB.clear();

            // iterate through all our values, binning each record based on what
            // it was tagged with
            // make sure to remove the tag!
            for (Text t : values) {
                if (t.charAt(0) == 'A') {
                    listA.add(new Text(t.toString().substring(1)));
                } else if (t.charAt(0) == 'B') {
                    listB.add(new Text(t.toString().substring(1)));
                }
            }

            // Execute our join logic now that the lists are filled
            executeJoinLogic(context);
        }

        private void executeJoinLogic(Context context) throws IOException,
                InterruptedException {
            if (joinType.equalsIgnoreCase("inner")) {
                // If both lists are not empty, join A with B
                if (!listA.isEmpty() && !listB.isEmpty()) {
                    for (Text A : listA) {
                        for (Text B : listB) {
                            context.write(A, B);
                        }
                    }
                }
            } else if (joinType.equalsIgnoreCase("leftouter")) {
                // For each entry in A,
                for (Text A : listA) {
                    // If list B is not empty, join A and B
                    if (!listB.isEmpty()) {
                        for (Text B : listB) {
                            context.write(A, B);
                        }
                    } else {
                        // Else, output A by itself
                        context.write(A, new Text(""));
                    }
                }
            } else if (joinType.equalsIgnoreCase("rightouter")) {
                // FOr each entry in B,
                for (Text B : listB) {
                    // If list A is not empty, join A and B
                    if (!listA.isEmpty()) {
                        for (Text A : listA) {
                            context.write(A, B);
                        }
                    } else {
                        // Else, output B by itself
                        context.write(new Text(""), B);
                    }
                }
            } else if (joinType.equalsIgnoreCase("fullouter")) {
                // If list A is not empty
                if (!listA.isEmpty()) {
                    // For each entry in A
                    for (Text A : listA) {
                        // If list B is not empty, join A with B
                        if (!listB.isEmpty()) {
                            for (Text B : listB) {
                                context.write(A, B);
                            }
                        } else {
                            // Else, output A by itself
                            context.write(A, new Text(""));
                        }
                    }
                } else {
                    // If list A is empty, just output B
                    for (Text B : listB) {
                        context.write(new Text(""), B);
                    }
                }
            } else if (joinType.equalsIgnoreCase("anti")) {
                // If list A is empty and B is empty or vice versa
                if (listA.isEmpty() ^ listB.isEmpty()) {

                    // Iterate both A and B with null values
                    // The previous XOR check will make sure exactly one of
                    // these lists is empty and therefore won't have output
                    for (Text A : listA) {
                        context.write(A, new Text(""));
                    }

                    for (Text B : listB) {
                        context.write(new Text(""), B);
                    }
                }
            } else {
                throw new RuntimeException(
                        "Join type not set to inner, leftouter, rightouter, fullouter, or anti");
            }
        }
    }

    public JoinTxinTxout() {}

    private static final String TXIN_INPUT = "txin";
    private static final String TXOUT_INPUT = "txout";
    private static final String OUTPUT = "output";
    private static final String JOIN_TYPE = "joinType";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("txin input path").create(TXIN_INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("txout input path").create(TXOUT_INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("string").hasArg().withDescription("join type").create(JOIN_TYPE));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(TXIN_INPUT) || !cmdline.hasOption(TXOUT_INPUT)
                || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(JOIN_TYPE)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String txinInputPath = cmdline.getOptionValue(TXIN_INPUT);
        String txoutInputPath = cmdline.getOptionValue(TXOUT_INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        String joinType = cmdline.getOptionValue(JOIN_TYPE);

        LOG.info("Tool name: " + JoinTxinTxout.class.getSimpleName());
        LOG.info(" - txinInputDir: " + txinInputPath);
        LOG.info(" - txoutInputDir: " + txoutInputPath);
        LOG.info(" - outputDir: " + outputPath);
        LOG.info(" - joinType: " + joinType);

        Configuration conf = getConf();
        conf.set("mapred.textoutputformat.separator", " ");

        Job job = Job.getInstance(getConf());
        job.setJobName(JoinTxinTxout.class.getName());
        job.setJarByClass(JoinTxinTxout.class);
        job.getConfiguration().set("join.type", joinType);

        MultipleInputs.addInputPath(job, new Path(txinInputPath),
                TextInputFormat.class, TxinJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(txoutInputPath),
                TextInputFormat.class, TxoutJoinMapper.class);

        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // Delete the output directory if it exists already.
        FileSystem.get(conf).delete(new Path(outputPath), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("\nJob Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        // Save execution time on disk
        MyUtils.generateReport(JoinTxinTxout.class.getSimpleName(), MyUtils.getCurrentDateTime(),System.currentTimeMillis() - startTime);

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new JoinTxinTxout(), args);
        System.exit(res);
    }
}
