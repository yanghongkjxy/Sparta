package com.stratio.sparkta.plugin.output.solr;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.annotation.ThreadSafe;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.pool.AbstractConnPool;
import org.apache.http.pool.ConnFactory;

/**
 * @since 4.3
 */
@ThreadSafe
class CPool extends AbstractConnPool<HttpRoute, ManagedHttpClientConnection, CPoolEntry> {

    private static final AtomicLong COUNTER = new AtomicLong();

    private final Log log = LogFactory.getLog(CPool.class);
    private final long timeToLive;
    private final TimeUnit tunit;

    public CPool(
            final ConnFactory<HttpRoute, ManagedHttpClientConnection> connFactory,
            final int defaultMaxPerRoute, final int maxTotal,
            final long timeToLive, final TimeUnit tunit) {
        super(connFactory, defaultMaxPerRoute, maxTotal);
        this.timeToLive = timeToLive;
        this.tunit = tunit;
    }

    @Override
    protected CPoolEntry createEntry(final HttpRoute route, final ManagedHttpClientConnection conn) {
        final String id = Long.toString(COUNTER.getAndIncrement());
        return new CPoolEntry(this.log, id, route, conn, this.timeToLive, this.tunit);
    }

    @Override
    protected boolean validate(final CPoolEntry entry) {
        return !entry.getConnection().isStale();
    }

}
