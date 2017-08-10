package com.mikolaj.app;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinTransaction;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinTransactionOutput;

import java.io.IOException;

/**
 * Mapper
 *
 */
public class BlockTxSumMapper extends Mapper<BytesWritable, BitcoinBlock, Text, LongWritable> {

    @Override
    public void map(BytesWritable key, BitcoinBlock value, Context context) throws IOException, InterruptedException {
        // get the sum of transactions in Satoshi
        byte[] prevHash = value.getHashPrevBlock();
        ArrayUtils.reverse(prevHash);
        long sum = 0;
        for (BitcoinTransaction tx : value.getTransactions()) {
            for (BitcoinTransactionOutput txOut : tx.getListOfOutputs()) {
                sum = sum + txOut.getValue();
            }
        }
        context.write(new Text(Hex.encodeHexString(prevHash)), new LongWritable(sum));
    }
}
