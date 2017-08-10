package com.mikolaj.app;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock;

import java.io.IOException;

/**
 * Mapper
 *
 */
public class TotalTxCountMapper extends Mapper<BytesWritable, BitcoinBlock, Text, IntWritable> {

    private static final Text defaultKey = new Text("Transaction Count:");

    @Override
    public void map(BytesWritable key, BitcoinBlock value, Context context) throws IOException, InterruptedException {
        // get the number of transactions
        context.write(defaultKey, new IntWritable(value.getTransactions().size()));
    }
}
