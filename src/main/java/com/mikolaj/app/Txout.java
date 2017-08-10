package com.mikolaj.app;

/**
 * Created by Mikolaj on 03.08.17.
 */
public class Txout {
    private long id;
    private int txoutIndex;
    private String pkScript;
    private long value;
    //private int type;
    private String typeStr;
    private String outAddress;
    private long tx_id; //FK

    public Txout(long id, int txoutIndex, String pkScript, long value, String typeStr, String outAddress, long tx_id) {
        this.id = id;
        this.txoutIndex = txoutIndex;
        this.pkScript = pkScript;
        this.value = value;
        //this.type = type;
        this.typeStr = typeStr;
        this.outAddress = outAddress;
        this.tx_id = tx_id;
    }

    public int getTxoutIndex() {
        return txoutIndex;
    }

    public void setTxoutIndex(int txoutIndes) {
        this.txoutIndex = txoutIndes;
    }

    public String getPkScript() {
        return pkScript;
    }

    public void setPkScript(String pkScript) {
        this.pkScript = pkScript;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

//    public int getType() {
//        return type;
//    }
//
//    public void setType(int type) {
//        this.type = type;
//    }

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
