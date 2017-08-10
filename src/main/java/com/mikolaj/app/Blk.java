package com.mikolaj.app;

import java.util.List;

/**
 * Created by Mikolaj on 03.08.17.
 */
public class Blk {
    private long id;
    private String blockHash;
    private String prevBlockHash;
    private long time;
    private int blockSize;

    public Blk(long id, String blockHash, String prevBlockHash, long time, int blockSize) {
        this.id = id;
        this.blockHash = blockHash;
        this.prevBlockHash = prevBlockHash;
        this.time = time;
        this.blockSize = blockSize;
    }

    public String getBlockHash() {
        return blockHash;
    }

    public void setBlockHash(String blockHash) {
        this.blockHash = blockHash;
    }

    public String getPrevBlockHash() {
        return prevBlockHash;
    }

    public void setPrevBlockHash(String prevBlockHash) {
        this.prevBlockHash = prevBlockHash;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public long getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
