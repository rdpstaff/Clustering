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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 *
 * @author fishjord
 */
public class BufferedRandomAccessFile extends RandomAccessFile {

    private byte[] buf;
    private long realPos;
    private int bufPos;
    private int bufEnd;
    private boolean sync = false;

    public BufferedRandomAccessFile(File f, String mode, int bufSize) throws IOException {
        super(f, mode);
        buf = new byte[bufSize];
        fillBuffer();
    }

    @Override
    public int skipBytes(int n) throws IOException{
        throw new UnsupportedOperationException();
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
    public int read(byte b[]) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int c = read();
        if (c == -1) {
            return -1;
        }
        b[off] = (byte)c;

        int i = 1;
        try {
            for (; i < len ; i++) {
                c = read();
                if (c == -1) {
                    break;
                }
                b[off + i] = (byte)c;
            }
        } catch (IOException ee) {
        }
        return i;
    }


    @Override
    public final void write(int b) throws IOException {
        if (bufPos >= buf.length) {
            fillBuffer();
        }

        //System.out.println("Writing " + b + " to buf_loc=" + bufPos + " realPos=" + realPos + " bufEnd=" + bufEnd);

        buf[bufPos++] = (byte) b;
        if (bufPos >= bufEnd) {
            bufEnd = bufPos;
        }

        //System.out.println("Write called with " + b);
        //System.out.println("bufPos is now " + bufPos);
        //System.out.println("bufEnd is now " + bufEnd);

        sync = true;
    }

    @Override
    public long getFilePointer() throws IOException {
        long l = realPos;
        return (l - bufEnd + bufPos);
    }

    @Override
    public void seek(long pos) throws IOException {
        int n = (int) (realPos - pos);
        //System.out.println("Seeking to " + pos + ", in the file I'm at " + realPos + " bufPos=" + bufPos + " bufEnd=" + bufEnd + " where i think I'd be in the buffer " + n);
        if (n >= 0 && n <= bufEnd) {
            bufPos = bufEnd - n;
            //System.out.println("Seeking to " + n + " in the buffer");
        } else {
            //System.out.println("Seeking out of the buffer");
            sync();
            super.seek(pos);
            invalidate();
        }
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        sync();
        super.close();
    }

    private void sync() throws IOException {
        if (sync) {
            super.seek(realPos - bufEnd);
            super.write(buf, 0, bufEnd);
            sync = false;
            //invalidate();
        }
    }

    private int fillBuffer() throws IOException {
        sync();

        int n = super.read(buf, 0, buf.length);
        if (n >= 0) {
            //System.out.println("Filling buffer...read in " + n + " bytes");
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

    public static void main(String[] args) throws Exception {
        try {
            File file = new File("tmp.bin");
            if (file.exists()) {
                file.delete();
            }

            BufferedRandomAccessFile f = new BufferedRandomAccessFile(file, "rw", 1);

            f.writeInt(1);
            f.writeInt(2);
            f.writeInt(3);
            f.writeInt(4);
            f.writeInt(5);

            f.seek(0);

            System.out.println(f.readInt());
            System.out.println(f.readInt());
            f.writeInt(6);
            System.out.println(f.readInt());
            System.out.println(f.readInt());

            f.seek(0);

            System.out.println(f.readInt());
            System.out.println(f.readInt());
            System.out.println(f.readInt());
            System.out.println(f.readInt());
            System.out.println(f.readInt());
            f.close();
        } catch (Exception e) {
        }
    }
}
