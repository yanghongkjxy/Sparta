/**
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.sparta.plugin.output.solr;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.SocketException;
import java.util.*;

public class SolrOutputWriter {

    private static final Logger log = LoggerFactory.getLogger(SolrOutputWriter.class);


    public static void insert(final DataFrame df, boolean overwrite, String zkHost, boolean skipDefaultIndex, String
            collection) {
        JavaRDD<SolrInputDocument> docs = df.javaRDD().map(new Function<Row, SolrInputDocument>() {
            public SolrInputDocument call(Row row) throws Exception {
                StructType schema = row.schema();
                SolrInputDocument doc = new SolrInputDocument();
                StructField[] fields = schema.fields();
                for (int i=0; i < fields.length; i++) {
                    StructField f = fields[i];
                    String fname = f.name();
                    if (fname.equals("_version_"))
                        continue;

                    Object val = !row.isNullAt(i) ? row.get(i) : null;
                    if (val != null)
                        doc.setField(fname, val);
                }
                return doc;
            }
        });
        indexDocs(zkHost, collection, skipDefaultIndex, 100, docs);
    }


    private static void indexDocs(final String zkHost,
                                 final String collection,
                                 final Boolean skipDefaultIndex,
                                 final int batchSize,
                                 JavaRDD<SolrInputDocument> docs) {

        docs.foreachPartition(
                new VoidFunction<Iterator<SolrInputDocument>>() {
                    public void call(Iterator<SolrInputDocument> solrInputDocumentIterator) throws Exception {
                        final SolrServer solrServer = getSolrServer(zkHost,collection);
                        List<SolrInputDocument> batch = new ArrayList<SolrInputDocument>();
                        Date indexedAt = new Date();
                        while (solrInputDocumentIterator.hasNext()) {
                            SolrInputDocument inputDoc = solrInputDocumentIterator.next();
                            if (!skipDefaultIndex)
                                inputDoc.setField("_indexed_at_tdt", indexedAt);
                            batch.add(inputDoc);
                            if (batch.size() >= batchSize)
                                sendBatchToSolr(solrServer, collection, batch);
                        }
                        if (!batch.isEmpty())
                            sendBatchToSolr(solrServer, collection, batch);
                        solrServer.commit();
                    }
                }
        );
    }

    private static Map<String,CloudSolrServer> solrServers = new HashMap<String, CloudSolrServer>();

    private static CloudSolrServer getSolrServer(String host, String collection) {
        CloudSolrServer solr = null;
        synchronized (solrServers) {
            String key = host + collection;
            solr = solrServers.get(key);
            if (solr == null) {
                solr = new CloudSolrServer(host);
                solr.setDefaultCollection(collection);
                solr.connect();
                solrServers.put(key, solr);
            }
        }
        return solr;
    }

    private static void sendBatchToSolr(SolrServer solrServer, String collection, Collection<SolrInputDocument> batch) {
        UpdateRequest req = new UpdateRequest();
        req.setParam("collection", collection);

        if (log.isDebugEnabled())
            log.debug("Sending batch of " + batch.size() + " to collection " + collection);

        req.add(batch);
        try {
            solrServer.request(req);
        } catch (Exception e) {
            if (shouldRetry(e)) {
                log.error("Send batch to collection "+collection+" failed due to "+e+"; will retry ...");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.interrupted();
                }

                try {
                    solrServer.request(req);
                } catch (Exception e1) {
                    log.error("Retry send batch to collection "+collection+" failed due to: "+e1, e1);
                    if (e1 instanceof RuntimeException) {
                        throw (RuntimeException)e1;
                    } else {
                        throw new RuntimeException(e1);
                    }
                }
            } else {
                log.error("Send batch to collection "+collection+" failed due to: "+e, e);
                if (e instanceof RuntimeException) {
                    throw (RuntimeException)e;
                } else {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            batch.clear();
        }
    }

    private static boolean shouldRetry(Exception exc) {
        Throwable rootCause = SolrException.getRootCause(exc);
        return (rootCause instanceof ConnectException ||
                rootCause instanceof SocketException);

    }

}
