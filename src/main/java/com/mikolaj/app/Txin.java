package com.mikolaj.app;

/**
 * Created by Mikolaj on 03.08.17.
 */
public class Txin {
    private long id;
    private int txinIndex;
    private String prevTransactionHash;
    private long prevTransactionIndex;
    private long tx_id; //FK

    public Txin(long id, int txinIndex, String prevTransactionHash, long prevTransactionIndex, long tx_id) {
        this.id = id;
        this.txinIndex = txinIndex;
        this.prevTransactionHash = prevTransactionHash;
        this.prevTransactionIndex = prevTransactionIndex;
        this.tx_id = tx_id; //FK
    }

    public int getTxinIndex() {
        return txinIndex;
    }

    public void setTxinIndex(int txinIndex) {
        this.txinIndex = txinIndex;
    }

    public String getPrevTransactionHash() {
        return prevTransactionHash;
    }

    public void setPrevTransactionHash(String prevTransactionHash) {
        this.prevTransactionHash = prevTransactionHash;
    }

    public long getPrevTransactionIndex() {
        return prevTransactionIndex;
    }

    public void setPrevTransactionIndex(long prevTransactionIndex) {
        this.prevTransactionIndex = prevTransactionIndex;
    }

    public long getTx_id() {
        return tx_id;
    }

    public void setTx_id(int tx_id) {
        this.tx_id = tx_id;
    }


    public long getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

}
