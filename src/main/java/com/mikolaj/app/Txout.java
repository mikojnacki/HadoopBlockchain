package com.mikolaj.app;

/**
 * Created by Mikolaj on 03.08.17.
 */
public class Txout {
    private long id;
    private int txoutIndex;
    private String outAddress;
    private long value;
    private String typeStr;
    private long tx_id; //FK

    public Txout(long id, int txoutIndex, String outAddress, long value, String typeStr,  long tx_id) {
        this.id = id;
        this.txoutIndex = txoutIndex;
        this.outAddress = outAddress;
        this.value = value;
        this.typeStr = typeStr;
        this.tx_id = tx_id;
    }

    public int getTxoutIndex() {
        return txoutIndex;
    }

    public void setTxoutIndex(int txoutIndes) {
        this.txoutIndex = txoutIndes;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
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

    public String getTypeStr() {
        return typeStr;
    }

    public void setTypeStr(String typeStr) {
        this.typeStr = typeStr;
    }

    public String getOutAddress() {
        return outAddress;
    }

    public void setOutAddress(String outAddress) {
        this.outAddress = outAddress;
    }
}
