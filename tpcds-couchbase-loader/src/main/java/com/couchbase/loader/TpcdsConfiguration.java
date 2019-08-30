/*
 * Copyright 2019 Couchbase, Inc.
 */
package com.couchbase.loader;

import com.teradata.tpcds.Session;

public class TpcdsConfiguration {
    private Session session;
    private int partition;
    private String tableToGenerate;

    public TpcdsConfiguration(Session session, int partition, String tableToGenerate) {
        this.session = session;
        this.partition = partition;
        this.tableToGenerate = tableToGenerate;
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public int getPartitions() {
        return session.getParallelism();
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public String getTableToGenerate() {
        return tableToGenerate;
    }

    public void setTableToGenerate(String tableToGenerate) {
        this.tableToGenerate = tableToGenerate;
    }
}
