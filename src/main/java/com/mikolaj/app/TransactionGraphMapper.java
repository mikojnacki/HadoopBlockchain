package com.mikolaj.app;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.Script;

import java.io.IOException;
import java.util.Arrays;

/**
 * Mapper
 *
 */
public class TransactionGraphMapper extends Mapper<BytesWritable, BytesWritable, Text, NullWritable> {

    private final org.bitcoinj.core.Context bitcoinContext = new org.bitcoinj.core.Context(MainNetParams.get());

    @Override
    public void map(BytesWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {

        // emit: <fromAddress> <toAddress> <value> <txHash>

        // omit first 8 bytes: magic bytes (4) and block size (4)
        byte[] valueByte = Arrays.copyOfRange(value.getBytes(), 8, value.getBytes().length);
        Block block = new Block(MainNetParams.get(), valueByte);
        //System.out.println(block.toString());
        //System.out.println(block.getTransactions().size());

        for (Transaction tx : block.getTransactions()) {
            // get txHash
            String txHash = tx.getHashAsString();

            for (TransactionInput txIn : tx.getInputs()) {
                // get inAddress
                String inAddress;
                try {
                    inAddress = txIn.getFromAddress().toBase58();
                } catch (ScriptException e) {
                    if (e.getMessage().equals("This is a coinbase transaction which generates new coins. It does not have a from address.")) {
                        inAddress = "coinbase";
                    } else {
                        inAddress = "unknown";
                    }
                }

                for (TransactionOutput txOut : tx.getOutputs()) {
                    // get outAddress and value in satoshi
                    String outAddress = ""; // initialize with empty debug string
                    long satoshis = 1; // to count number of NO_TYPE unhandled exceptions
                    switch (txOut.getScriptPubKey().getScriptType()) {
                        case P2PKH:
                            Address outAddressP2PLH = txOut.getAddressFromP2PKHScript(MainNetParams.get());
                            outAddress = outAddressP2PLH.toBase58();
                            satoshis = txOut.getValue().value;
                            break;
                        case P2SH:
                            Address outAddressP2SH = txOut.getAddressFromP2SH(MainNetParams.get());
                            outAddress = outAddressP2SH.toBase58();
                            satoshis = txOut.getValue().value;
                            break;
                        case PUB_KEY:
                            Script outScriptPK = txOut.getScriptPubKey();
                            byte[] byteOutAddressPK = outScriptPK.getPubKey();
                            Address outAddressPK = new Address(MainNetParams.get(), Utils.sha256hash160(byteOutAddressPK));
                            outAddress = outAddressPK.toBase58();
                            satoshis = txOut.getValue().value;
                            break;
                        default: //NO_TYPE and others if exist
                            Script outScriptNT = txOut.getScriptPubKey();
                            try {
                                Script outScript = new Script(Arrays.copyOfRange(outScriptNT.getProgram(), 0, 25));
                                Address outAddressNT = outScript.getToAddress(MainNetParams.get());
                                outAddress = outAddressNT.toBase58();
                                satoshis = txOut.getValue().value;
                            } catch (ScriptException e) {
                                System.out.println("Exception found in NO_TYPE script:");
                                System.out.println(txOut.toString());
                            }
                            break;
                    }
//                    context.write(new Text(inAddress + "\t" + outAddress + "\t" + satoshis + "\t" + txHash),
//                            NullWritable.get());
                    // without info about transaction hash
                    context.write(new Text(inAddress + "\t" + outAddress + "\t" + satoshis),
                            NullWritable.get());
                }

            }
        }

    }
}
