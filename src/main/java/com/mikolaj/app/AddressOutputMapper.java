package com.mikolaj.app;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.Script;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinTransaction;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinTransactionOutput;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinUtil;

import java.io.IOException;

/**
 * Mapper
 *
 * TODO: properly handle PUB_KEY and NO_TYPE type of signatures - see AddressOutputRawMapper.java
 *
 */
public class AddressOutputMapper extends Mapper<BytesWritable, BitcoinBlock, Text, LongWritable> {

    @Override
    public void map(BytesWritable key, BitcoinBlock value, Context context) throws IOException, InterruptedException {
        // emit output address as key and Satoshi value credited to it as value
        String debugBase58 = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
        for (BitcoinTransaction tx : value.getTransactions()) {
            for (BitcoinTransactionOutput txOut : tx.getListOfOutputs()) {
                Text address = new Text(debugBase58);
                Script outScript = new Script(txOut.getTxOutScript());
                switch (outScript.getScriptType()) {
                    case P2PKH:
                        Address outAddressP2PKH = outScript.getToAddress(MainNetParams.get());
                        address = new Text(outAddressP2PKH.toBase58());
                        break;
                    case P2SH:
                        byte[] outAddressP2SH = outScript.getPubKeyHash();
                        address = new Text(Hex.encodeHexString(outAddressP2SH));
                        break;
                    case PUB_KEY:
                        byte[] outAddressPK = outScript.getPubKey();
                        address = new Text(Hex.encodeHexString(outAddressPK));
                        break;
                    case NO_TYPE:
                        System.out.println("NO_TYPE: Unknown type of script");
                        break;
                    default:
                        System.out.println("DEFAULT CLAUSE");
                        break;
                }
                context.write(address, new LongWritable(txOut.getValue()));
            }
        }

//        String debugBase58 = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
//        for (BitcoinTransaction tx : value.getTransactions()) {
//            for (BitcoinTransactionOutput txOut : tx.getListOfOutputs()) {
//                Script outScript = new Script(txOut.getTxOutScript());
//                switch (outScript.getScriptType()) {
//                    case P2PKH:
//                        Address outAddressP2PKH = outScript.getToAddress(MainNetParams.get());
//                        context.write(new Text(outAddressP2PKH.toBase58()), new LongWritable(txOut.getValue()));
//                        break;
//                    case P2SH:
//                        byte[] outAddressP2SH = outScript.getPubKeyHash();
//                        context.write(new Text(Hex.encodeHexString(outAddressP2SH)), new LongWritable(txOut.getValue()));
//                        break;
//                    case PUB_KEY:
//                        byte[] outAddressPK = outScript.getPubKey();
//                        context.write(new Text(Hex.encodeHexString(outAddressPK)), new LongWritable(txOut.getValue()));
//                        break;
//                    case NO_TYPE:
//                        System.out.println("NO_TYPE: Unknown type of script");
//                        context.write(new Text(debugBase58), new LongWritable(txOut.getValue()));
//                        break;
//                    default:
//                        System.out.println("DEFAULT CLAUSE");
//                        break;
//                }
//                //Address outAddress = outScript.getToAddress(MainNetParams.get());
//                //context.write(new Text(outAddress.toString()), new LongWritable(txOut.getValue()));
//            }
//        }


//        String debugBase58 = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
//        Block block = new Block(MainNetParams.get(), value.getBits());
//        for (Transaction tx : block.getTransactions()) {
//            for (TransactionOutput txOut : tx.getOutputs()) {
//
//                Address outAddress = new Address(MainNetParams.get(), debugBase58);
//                switch (txOut.getScriptPubKey().getScriptType()) {
//                    case P2PKH:
//                        outAddress = txOut.getAddressFromP2PKHScript(MainNetParams.get());
//                        break;
//                    case P2SH:
//                        outAddress = txOut.getAddressFromP2SH(MainNetParams.get());
//                        break;
//                    default:
//                        //outAddress = null;
//                        System.out.println("Invalid script. Cannot extract the address");
//                        break;
//                }
//                context.write(new Text(outAddress.toBase58()), new LongWritable(txOut.getValue().value));
//            }
//        }
    }
}
