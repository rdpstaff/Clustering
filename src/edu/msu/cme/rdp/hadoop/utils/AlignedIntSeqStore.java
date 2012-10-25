/*
 * Ribosomal Database Project II  http://rdp.cme.msu.edu
 * Copyright 2009, Michigan State University Board of Trustees
 *
 * This software is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The Ribosomal Database Project II and Michigan State University
 * distributes this software in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this software.  If not, see <http://www.gnu.org/licenses/>.
 */

package edu.msu.cme.rdp.hadoop.utils;

import java.io.DataInput;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Intended to store sequences that are aligned, aka they are all the same length.
 * Allows for accessing sequences by directly jumping to a position in the file
 * without having to stream over the entire file.
 *
 * File Structure:
 * long numOfSeqs
 * int lengthOfAlign
 * for each seq:
 *   int id
 *   byte[lengthOfAlign] data
 *
 * CLOSE MUST BE CALLED OR THE SEQSTORE WILL NOT BE READABLE.
 *
 * @author farrisry
 */
public class AlignedIntSeqStore {
    final RandomAccessFile file;
    long seqCount = 0;
    int alignLength = -1;

    public AlignedIntSeqStore(File file) throws IOException {
        this.file = new RandomAccessFile(file, "rw");
        this.file.writeLong(Long.MIN_VALUE);
        this.file.writeInt(Integer.MIN_VALUE);
    }


    public void addSeq(IntSeq seq) {
        try {
            file.writeInt(seq.getId());
            if (alignLength == -1) {
                alignLength = seq.getBytes().length;
            } else if (alignLength != seq.getBytes().length) {
                throw new RuntimeException("Sequence length is not the same! Cannot store in AlignedSeqStore: " + seq.getId());
            }
            file.write(seq.getBytes());
            seqCount++;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }


    public void close() throws IOException {
        file.seek(0);
        file.writeLong(seqCount);
        file.writeInt(alignLength);
        file.close();
    }

    
    public static List<IntSeq> fromFile(File file, long offset, long count) throws IOException {
        RandomAccessFile di = null;
        try {
            di = new RandomAccessFile(file, "r");
            long numSeqs = di.readLong();
            int seqLength = di.readInt();

            List<IntSeq> seqs = new ArrayList<IntSeq>();

            if (offset + count > numSeqs) {
                count = numSeqs - offset;
            }

            long byteOffset = (Long.SIZE/8) + (Integer.SIZE/8) + offset * ((Integer.SIZE/8) + seqLength); // offset from beginning of file
            di.seek(byteOffset);

            for (long i = 0; i < count; i++) {
                int id = di.readInt();
                byte[] bytes = new byte[seqLength];
                di.read(bytes);
                seqs.add(new IntSeq(id, bytes));
            }
            return seqs;
        } finally {
            try {
                di.close();
            } catch (IOException ex) {
                Logger.getLogger(AlignedIntSeqStore.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }


    public static long numberOfSeqs(File file) throws IOException {
        RandomAccessFile di = new RandomAccessFile(file, "r");
        try {
            return di.readLong();
        } finally {
            if (di != null) {
                di.close();
            }
        }
    }


    public static long numberOfSeqs(DataInput di) {
        try {
            long numSeqs = di.readLong();
            if (numSeqs <= 0) {
                throw new RuntimeException("Sequence store file corrupt.");
            }
            int seqLength = di.readInt();
            byte[] bases = new byte[seqLength];
            for (long i = 0; i < numSeqs; i++) {
                int id = di.readInt();
                di.readFully(bases);
            }
            return numSeqs;
            
        } catch (EOFException ex) {
            throw new RuntimeException("Sequence store file corrupt.");

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void main(String ... args) throws Exception {
        File file = new File("/tmp/bozo.bin");
        AlignedIntSeqStore ss = new AlignedIntSeqStore(file);
        ss.addSeq(IntSeq.createSeq(0, "ACTGACTG"));
        ss.addSeq(IntSeq.createSeq(1, "AGGGGGGG"));
        ss.addSeq(IntSeq.createSeq(2, "CCCCCCCC"));
        ss.addSeq(IntSeq.createSeq(3, "TTTTTTTT"));
        ss.addSeq(IntSeq.createSeq(4, "TTTTGGGG"));
        ss.close();

        List<IntSeq> seqs = ss.fromFile(file, 0, 200);
        for (IntSeq s : seqs) {
            System.out.println(s.getId() + " ");// + s.getBases());
        }

        System.out.println(AlignedIntSeqStore.numberOfSeqs(file));
    }
}
