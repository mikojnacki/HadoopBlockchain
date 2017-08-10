package com.mikolaj.app;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Mikolaj on 03.08.17.
 */
public class Parsed {
    private List<Blk> blks;
    private List<Tx> txs;
    private List<Txin> txins;
    private List<Txout> txouts;

    public Parsed(List<Blk> blks, List<Tx> txs, List<Txin> txins, List<Txout> txouts) {
        this.blks = blks;
        this.txs = txs;
        this.txins = txins;
        this.txouts = txouts;
    }

    public List<Blk> getBlks() {
        return blks;
    }

    public void setBlks(List<Blk> blks) {
        this.blks = blks;
    }

    public List<Tx> getTxs() {
        return txs;
    }

    public void setTxs(List<Tx> txs) {
        this.txs = txs;
    }

    public List<Txin> getTxins() {
        return txins;
    }

    public void setTxins(List<Txin> txins) {
        this.txins = txins;
    }

    public List<Txout> getTxouts() {
        return txouts;
    }

    public void setTxouts(List<Txout> txouts) {
        this.txouts = txouts;
    }
}
