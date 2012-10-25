/*
 * Copyright (C) 2012 Michigan State University <rdpstaff at msu.edu>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package edu.msu.cme.pyro.cluster.upgma;

import edu.msu.cme.pyro.cluster.dist.ThinEdge;
import edu.msu.cme.pyro.cluster.io.LocalEdgeReader;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;

/**
 *
 * @author fishjord
 */
public class RandomAccessEdgeFile {

    /*public static class BufferedRandomAccessFile extends RandomAccessFile {

        private byte[] buf;
        private long realPos;
        private int bufPos;
        private int bufEnd;

        public BufferedRandomAccessFile(File f, String mode, int bufSize) throws IOException {
            super(f, mode);
            invalidate();
            buf = new byte[bufSize];
        }

        @Override
        public final int read() throws IOException {
            if (bufPos >= bufEnd) {
                if (fillBuffer() < 0) {
                    return -1;
                }
            }

            if (bufEnd == 0) {
                return -1;
            } else {
                return buf[bufPos++] & 0xff;
            }
        }

        @Override
        public long getFilePointer() throws IOException {
            long l = realPos;
            return (l - bufEnd + bufPos);
        }

        @Override
        public void seek(long pos) throws IOException {
            int n = (int) (realPos - pos);
            if (n >= 0 && n <= bufEnd) {
                bufPos = bufEnd - n;
            } else {
                super.seek(pos);
                invalidate();
            }
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            throw new UnsupportedOperationException();
        }

        private int fillBuffer() throws IOException {
            int n = super.read(buf, 0, buf.length);
            if (n >= 0) {
                realPos += n;
                bufEnd = n;
                bufPos = 0;
            }
            return n;
        }

        private void invalidate() throws IOException {
            bufEnd = 0;
            bufPos = 0;
            realPos = super.getFilePointer();
        }
    }*/

    private RandomAccessFile edgeFile;
    private File f;
    private long lastRead = 0;
    private long mark = -1;

    public RandomAccessEdgeFile(File f) throws IOException {
        this.f = f;
        this.edgeFile = new BufferedRandomAccessFile(f, "rw", 1024);
        //edgeFile = new RandomAccessFile(f, "rw");
    }

    public RandomAccessEdgeFile(File f, RandomAccessFile raf) throws IOException {
        this.f = f;
        //this.edgeFile = new BufferedRandomAccessFile(f, "rw", 1024);
        //edgeFile = new RandomAccessFile(f, "rw");
        this.edgeFile = raf;
    }

    public void delete() {
        f.delete();
    }

    public void mark() throws IOException {
        mark = edgeFile.getFilePointer();
    }

    public void reset() throws IOException {
        if (mark == -1) {
            throw new IOException("Mark not set!");
        }

        edgeFile.seek(mark);
        mark = -1;
    }

    public ThinEdge nextThinEdge() throws IOException {
        lastRead = edgeFile.getFilePointer();
        try {
            int seqi = edgeFile.readInt();
            int seqj = edgeFile.readInt();
            int dist = edgeFile.readInt();

            return new ThinEdge(seqi, seqj, dist);
        } catch (EOFException e) {
            return null;
        }
    }

    public void overwriteEdge(ThinEdge edge) throws IOException {
        edgeFile.seek(lastRead);
        edgeFile.writeInt(edge.getSeqi());
        edgeFile.writeInt(edge.getSeqj());
        edgeFile.writeInt(edge.getDist());
    }

    public void close() throws IOException {
        edgeFile.close();
    }

    public static void runTests(File f, PrintStream out) throws IOException {
        /*RandomAccessFile raf = new RandomAccessFile(f, "rw");

        RandomAccessEdgeFile raef = new RandomAccessEdgeFile(f, raf);
        long startTime = System.currentTimeMillis();
        while(raef.nextThinEdge() != null);
        System.out.println("Edges drained in " + (System.currentTimeMillis() - startTime) + "ms");
        out.println("0\t" + (System.currentTimeMillis() - startTime));
        raf.close();*/

        ThinEdge edge;
        RandomAccessFile raf = new BufferedRandomAccessFile(f, "rw", 1024);
        RandomAccessEdgeFile raef = new RandomAccessEdgeFile(f, raf);
        long startTime = System.currentTimeMillis();
        while((edge = raef.nextThinEdge()) != null) {
            //System.out.println(edge.getSeqi() + " " + edge.getSeqj() + ": " + edge.getDist());
        }
        System.out.println("Edges drained in " + (System.currentTimeMillis() - startTime) + "ms using buffered edge reader with bufSize=1024");
        out.println("1024\t" + (System.currentTimeMillis() - startTime));
        raf.close();

        raf = new BufferedRandomAccessFile(f, "rw", 2048);
        raef = new RandomAccessEdgeFile(f, raf);
        startTime = System.currentTimeMillis();
        while(raef.nextThinEdge() != null);
        System.out.println("Edges drained in " + (System.currentTimeMillis() - startTime) + "ms using buffered edge reader with bufSize=2048");
        out.println("2048\t" + (System.currentTimeMillis() - startTime));
        raf.close();

        raf = new BufferedRandomAccessFile(f, "rw", 4096);
        raef = new RandomAccessEdgeFile(f, raf);
        startTime = System.currentTimeMillis();
        while(raef.nextThinEdge() != null);
        System.out.println("Edges drained in " + (System.currentTimeMillis() - startTime) + "ms using buffered edge reader with bufSize=4096");
        out.println("4096\t" + (System.currentTimeMillis() - startTime));
        raf.close();

        raf = new BufferedRandomAccessFile(f, "rw", 8192);
        raef = new RandomAccessEdgeFile(f, raf);
        startTime = System.currentTimeMillis();
        while(raef.nextThinEdge() != null);
        System.out.println("Edges drained in " + (System.currentTimeMillis() - startTime) + "ms using buffered edge reader with bufSize=4096");
        out.println("8192\t" + (System.currentTimeMillis() - startTime));
        raf.close();

        raf = new BufferedRandomAccessFile(f, "rw", 32768);
        raef = new RandomAccessEdgeFile(f, raf);
        startTime = System.currentTimeMillis();
        while(raef.nextThinEdge() != null);
        System.out.println("Edges drained in " + (System.currentTimeMillis() - startTime) + "ms using buffered edge reader with bufSize=4096");
        out.println("32768\t" + (System.currentTimeMillis() - startTime));
        raf.close();

        LocalEdgeReader reader = new LocalEdgeReader(f);
        startTime = System.currentTimeMillis();
        while(reader.nextThinEdge() != null);
	System.out.println("Edges read in " + (System.currentTimeMillis() - startTime) + " ms using edge reader");
        out.println("buffered dis\t" + (System.currentTimeMillis() - startTime));
	reader.close();
    }

    public static void main(String [] args) throws Exception{
        File nfsFile = new File("/scratch/fishjord/really_big.matrix");
        File localFile = new File("/local/really_big.matrix");

        PrintStream out = new PrintStream("timing_stats.txt");

	for(int index = 0;index < 10;index++) {
		out.println("Iteration " + index + " NFS");
		runTests(nfsFile, out);
		out.println("Iteration " + index + " local");
		runTests(localFile, out);
	}
	out.close();
    }
}
