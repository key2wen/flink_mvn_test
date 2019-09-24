package com.key2wen.hive;

public class RedisDataModel {

    private int expire;
    private String key;
    private boolean global;
    private String value;

    public void setExpire(int expire) {
        this.expire = expire;
    }

    public void setKey(String key) {

        this.key = key;
    }

    public void setGlobal(boolean global) {
        this.global = global;

    }

    public void setValue(String value) {
        this.value = value;

    }
}
