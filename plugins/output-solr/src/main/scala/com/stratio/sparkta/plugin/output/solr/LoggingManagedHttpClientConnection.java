/*
 * ====================================================================
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 *
 */

package com.stratio.sparkta.plugin.output.solr;

import org.apache.commons.logging.Log;
import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.annotation.NotThreadSafe;
import org.apache.http.config.MessageConstraints;
import org.apache.http.entity.ContentLengthStrategy;
import org.apache.http.impl.conn.DefaultManagedHttpClientConnection;
import org.apache.http.impl.conn.Wire;
import org.apache.http.io.HttpMessageParserFactory;
import org.apache.http.io.HttpMessageWriterFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

@NotThreadSafe
class LoggingManagedHttpClientConnection extends DefaultManagedHttpClientConnection {

    class LoggingOutputStream extends OutputStream {

        private final OutputStream out;
        private final Wire wire;

        public LoggingOutputStream(final OutputStream out, final Wire wire) {
            super();
            this.out = out;
            this.wire = wire;
        }

        @Override
        public void write(final int b) throws IOException {
            try {
                wire.output(b);
            } catch (IOException ex) {
                wire.output("[write] I/O error: " + ex.getMessage());
                throw ex;
            }
        }

        @Override
        public void write(final byte[] b) throws IOException {
            try {
                wire.output(b);
                out.write(b);
            } catch (IOException ex) {
                wire.output("[write] I/O error: " + ex.getMessage());
                throw ex;
            }
        }

        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            try {
                wire.output(b, off, len);
                out.write(b, off, len);
            } catch (IOException ex) {
                wire.output("[write] I/O error: " + ex.getMessage());
                throw ex;
            }
        }

        @Override
        public void flush() throws IOException {
            try {
                out.flush();
            } catch (IOException ex) {
                wire.output("[flush] I/O error: " + ex.getMessage());
                throw ex;
            }
        }

        @Override
        public void close() throws IOException {
            try {
                out.close();
            } catch (IOException ex) {
                wire.output("[close] I/O error: " + ex.getMessage());
                throw ex;
            }
        }

    }



    class LoggingInputStream extends InputStream {

        private final InputStream in;
        private final Wire wire;

        public LoggingInputStream(final InputStream in, final Wire wire) {
            super();
            this.in = in;
            this.wire = wire;
        }

        @Override
        public int read() throws IOException {
            try {
                final int b = in.read();
                if (b == -1) {
                    wire.input("end of stream");
                } else {
                    wire.input(b);
                }
                return b;
            } catch (IOException ex) {
                wire.input("[read] I/O error: " + ex.getMessage());
                throw ex;
            }
        }

        @Override
        public int read(final byte[] b) throws IOException {
            try {
                final int bytesRead = in.read(b);
                if (bytesRead == -1) {
                    wire.input("end of stream");
                } else if (bytesRead > 0) {
                    wire.input(b, 0, bytesRead);
                }
                return bytesRead;
            } catch (IOException ex) {
                wire.input("[read] I/O error: " + ex.getMessage());
                throw ex;
            }
        }

        @Override
        public int read(final byte[] b, final int off, final int len) throws IOException {
            try {
                final int bytesRead = in.read(b, off, len);
                if (bytesRead == -1) {
                    wire.input("end of stream");
                } else if (bytesRead > 0) {
                    wire.input(b, off, bytesRead);
                }
                return bytesRead;
            } catch (IOException ex) {
                wire.input("[read] I/O error: " + ex.getMessage());
                throw ex;
            }
        }

        @Override
        public long skip(final long n) throws IOException {
            try {
                return super.skip(n);
            } catch (IOException ex) {
                wire.input("[skip] I/O error: " + ex.getMessage());
                throw ex;
            }
        }

        @Override
        public int available() throws IOException {
            try {
                return in.available();
            } catch (IOException ex) {
                wire.input("[available] I/O error : " + ex.getMessage());
                throw ex;
            }
        }

        @Override
        public void mark(final int readlimit) {
            super.mark(readlimit);
        }

        @Override
        public void reset() throws IOException {
            super.reset();
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void close() throws IOException {
            try {
                in.close();
            } catch (IOException ex) {
                wire.input("[close] I/O error: " + ex.getMessage());
                throw ex;
            }
        }

    }


    private final Log log;
    private final Log headerlog;
    private final Wire wire;

    public LoggingManagedHttpClientConnection(
            final String id,
            final Log log,
            final Log headerlog,
            final Log wirelog,
            final int buffersize,
            final int fragmentSizeHint,
            final CharsetDecoder chardecoder,
            final CharsetEncoder charencoder,
            final MessageConstraints constraints,
            final ContentLengthStrategy incomingContentStrategy,
            final ContentLengthStrategy outgoingContentStrategy,
            final HttpMessageWriterFactory<HttpRequest> requestWriterFactory,
            final HttpMessageParserFactory<HttpResponse> responseParserFactory) {
        super(id, buffersize, fragmentSizeHint, chardecoder, charencoder,
                constraints, incomingContentStrategy, outgoingContentStrategy,
                requestWriterFactory, responseParserFactory);
        this.log = log;
        this.headerlog = headerlog;
        this.wire = new Wire(wirelog, id);
    }

    @Override
    public void close() throws IOException {
        if (this.log.isDebugEnabled()) {
            this.log.debug(getId() + ": Close connection");
        }
        super.close();
    }

    @Override
    public void shutdown() throws IOException {
        if (this.log.isDebugEnabled()) {
            this.log.debug(getId() + ": Shutdown connection");
        }
        super.shutdown();
    }

    @Override
    protected InputStream getSocketInputStream(final Socket socket) throws IOException {
        InputStream in = super.getSocketInputStream(socket);
        if (this.wire.enabled()) {
            in = new LoggingInputStream(in, this.wire);
        }
        return in;
    }

    @Override
    protected OutputStream getSocketOutputStream(final Socket socket) throws IOException {
        OutputStream out = super.getSocketOutputStream(socket);
        if (this.wire.enabled()) {
            out = new LoggingOutputStream(out, this.wire);
        }
        return out;
    }

    @Override
    protected void onResponseReceived(final HttpResponse response) {
        if (response != null && this.headerlog.isDebugEnabled()) {
            this.headerlog.debug(getId() + " << " + response.getStatusLine().toString());
            final Header[] headers = response.getAllHeaders();
            for (final Header header : headers) {
                this.headerlog.debug(getId() + " << " + header.toString());
            }
        }
    }

    @Override
    protected void onRequestSubmitted(final HttpRequest request) {
        if (request != null && this.headerlog.isDebugEnabled()) {
            this.headerlog.debug(getId() + " >> " + request.getRequestLine().toString());
            final Header[] headers = request.getAllHeaders();
            for (final Header header : headers) {
                this.headerlog.debug(getId() + " >> " + header.toString());
            }
        }
    }

}
