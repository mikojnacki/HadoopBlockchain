package com.mikolaj.app;

/**
 * Created by Mikolaj on 03.08.17.
 */
public class Tx {
    private long id;
    private String transactionHash;
    private int transactionSize;
    private boolean coinbase;
    private long lockTime;
    private long blk_id;

    public Tx(long id, String transactionHash, int transactionSize, boolean coinbase, long lockTime, long blk_id) {
        this.id = id;
        this.transactionHash = transactionHash;
        this.transactionSize = transactionSize;
        this.coinbase = coinbase;
        this.lockTime = lockTime;
        this.blk_id = blk_id;
    }

    public String getTransactionHash() {
        return transactionHash;
    }

    public void setTransactionHash(String transactionHash) {
        this.transactionHash = transactionHash;
    }

    public int getTransactionSize() {
        return transactionSize;
    }

    public void setTransactionSize(int transactionSize) {
        this.transactionSize = transactionSize;
    }

    public boolean isCoinbase() {
        return coinbase;
    }

    public void setCoinbase(boolean coinbase) {
        this.coinbase = coinbase;
    }

    public long getBlk_id() {
        return blk_id;
    }

    public void setBlk_id(int blk_id) {
        this.blk_id = blk_id;
    }


    public long getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getLockTime() {
        return lockTime;
    }

    public void setLockTime(long lockTime) {
        this.lockTime = lockTime;
    }
}
