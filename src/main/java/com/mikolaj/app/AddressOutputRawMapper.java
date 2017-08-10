package com.mikolaj.app;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.Script;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinTransaction;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinTransactionOutput;
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinUtil;

import java.io.IOException;
import java.util.Arrays;

/**
 * Mapper
 *
 */
public class AddressOutputRawMapper extends Mapper<BytesWritable, BytesWritable, Text, LongWritable> {

    private final org.bitcoinj.core.Context bitcoinContext = new org.bitcoinj.core.Context(MainNetParams.get());

    @Override
    public void map(BytesWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {

        // emit output address as key and Satoshi value credited to it as value

        // omit first 8 bytes: magic bytes (4) and block size (4)
        byte[] valueByte = Arrays.copyOfRange(value.getBytes(), 8, value.getBytes().length);
        String debugBase58 = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
        //System.out.println(Hex.encodeHexString(value.getBytes()));
        Block block = new Block(MainNetParams.get(), valueByte);
        //System.out.println(block.toString());
        //System.out.println(block.getTransactions().size());
        for (Transaction tx : block.getTransactions()) {
            //System.out.println(tx.toString());
            for (TransactionOutput txOut : tx.getOutputs()) {
                Text address = new Text(debugBase58); // initialize with debug string
                LongWritable satoshis = new LongWritable(1); // to count number of NO_TYPE exceptions
                switch (txOut.getScriptPubKey().getScriptType()) {
                    case P2PKH:
                        Address outAddressP2PLH = txOut.getAddressFromP2PKHScript(MainNetParams.get());
                        address = new Text(outAddressP2PLH.toBase58());
                        satoshis = new LongWritable(txOut.getValue().value);
                        break;
                    case P2SH:
                        Address outAddressP2SH = txOut.getAddressFromP2SH(MainNetParams.get());
                        address = new Text(outAddressP2SH.toBase58());
                        satoshis = new LongWritable(txOut.getValue().value);
                        break;
                    case PUB_KEY:
                        Script outScriptPK = txOut.getScriptPubKey();
                        byte[] byteOutAddressPK = outScriptPK.getPubKey();
                        Address outAddressPK = new Address(MainNetParams.get(), Utils.sha256hash160(byteOutAddressPK));
                        address = new Text(outAddressPK.toBase58());
                        satoshis = new LongWritable(txOut.getValue().value);
                        break;
                    default: //NO_TYPE and others if exist
                        Script outScriptNT = txOut.getScriptPubKey();
                        try {
                            Script outScript = new Script(Arrays.copyOfRange(outScriptNT.getProgram(), 0, 25));
                            Address outAddress = outScript.getToAddress(MainNetParams.get());
                            address = new Text(outAddress.toBase58());
                            satoshis = new LongWritable(txOut.getValue().value);
                        } catch (ScriptException e) {
                            //System.out.println("Exception found in NO_TYPE script:");
                            //System.out.println(txOut.toString());
                        }
                        break;
                }
                context.write(address, satoshis);
            }
            // for tests - getting address from TransactionInput
//            for (TransactionInput txIn : tx.getInputs()) {
//                try {
//                    System.out.println(txIn.getFromAddress());
//                } catch (Exception e) {
//                    System.out.println("Getting an input address has failed");
//                }
//            }
        }
    }
}
