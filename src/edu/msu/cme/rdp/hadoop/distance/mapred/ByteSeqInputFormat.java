/*
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

package edu.msu.cme.rdp.hadoop.distance.mapred;

import edu.msu.cme.rdp.hadoop.distance.mapred.keys.MatrixRange;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Breaks the distance matrix up into subsections for the mappers to work on.
 *
 * Each Mapper has access to the entire sequence file via the DistributedCache.
 * Each key fed to the mapper is a subsection of the distance matrix to compute.
 *
 * @author farrisry
 */
public class ByteSeqInputFormat implements InputFormat<MatrixRange,NullWritable> {

    /**
     * This class does not have access to the sequence file, but it needs to know
     * the number of sequences to compute the matrix size.  That number is placed in the
     * JobConf under the NUM_SEQS_KEY key by the main driver class.
     */
    public static String NUM_SEQS_KEY = "rdp.dist.numOfSeqs";

    /**
     * size of the offset for the matrix ranges.  The number of sequences
     * a mapper must hold in memory is two times this amount.  Higher record sizes
     * means less time spent starting up mappers, but more memory.  Can be overriden
     * by passing the option -Drdp.dist.recordSize=NNNN to the ./bin/hadoop jar command.
     */
    public static String RECORD_SIZE_KEY = "rdp.dist.recordSize";
    static final int DEFAULT_RECORD_SIZE = 2000;

    /**
     * Number of matrix ranges to use per InputSplit.
     */
    public static String RANGES_PER_SPLIT_KEY = "rdp.dist.rangesPerSplit";
    static final int DEFAULT_RANGES_PER_SPLIT = 100;


    public void validateInput(JobConf conf) throws IOException {
        // do nothing.  This method has been deprecated in the hadoop api
    }

    /**
     * Breaks the distance matrix generation up into smaller subsections.
     *
     * @param conf JobConf must have NUM_SEQS_KEY value set to the number of sequences.
     * @param suggestedSplits ignored unless the matrix is small relative to the suggested size.
     * @return
     * @throws IOException
     */
    public InputSplit[] getSplits(JobConf conf, int suggestedSplits) throws IOException {
        long numSeqs = conf.getLong(NUM_SEQS_KEY, Long.MIN_VALUE);
        if (numSeqs <= 0) {
            throw new RuntimeException(NUM_SEQS_KEY + " was not set.  Cannot continue.");
        }
        int inMemSeqs = conf.getInt(RECORD_SIZE_KEY, DEFAULT_RECORD_SIZE);

        long rangesPerSplit = conf.getLong(RANGES_PER_SPLIT_KEY, DEFAULT_RANGES_PER_SPLIT);
        // figure out number of ranges per split to use
        long numRanges = (numSeqs / inMemSeqs) * (numSeqs / inMemSeqs) / 2;
        long numSplits = Math.max(numRanges / rangesPerSplit, 1);
        if (numSplits < suggestedSplits) {
            rangesPerSplit = numRanges / numSplits;
            System.out.println("ByteSeqInputFormat: Overriding rangesPerSplit due to suggestedSplits = " + suggestedSplits);
            System.out.println("ByteSeqInputFormat: rangesPerSplit = " + rangesPerSplit);
        }


        List<InputSplit> splits = new ArrayList<InputSplit>();
        SeqInputSplit split = new SeqInputSplit(new ArrayList<MatrixRange>());
        splits.add(split);
        for (long x=0; x < numSeqs; x += inMemSeqs) {
            for (long y=x; y < numSeqs; y += inMemSeqs) {
                MatrixRange mr = new MatrixRange();
                mr.set(x, y, inMemSeqs);
                if (split.ranges.size() >= rangesPerSplit) {
                    split = new SeqInputSplit(new ArrayList<MatrixRange>());
                    splits.add(split);
                }
                split.ranges.add(mr);
            }
        }

        System.out.println("ByteSeqInputFormat: number of splits = " + splits.size() + ", number of ranges per split = " + rangesPerSplit);
        return splits.toArray(new SeqInputSplit[splits.size()]);
    }

    /**
     * Breaks an InputSplit up into individual key,values to feed to a mapper.
     *
     * @param splitter SeqInputSplit to be used.
     * @param conf ignored
     * @param report ignored
     * @return
     * @throws IOException
     */
    public RecordReader<MatrixRange, NullWritable> getRecordReader(InputSplit splitter, JobConf conf, Reporter report) throws IOException {
        SeqInputSplit split = (SeqInputSplit)splitter;
        return new SeqRecordReader(split);
    }


    /**
     *
     */
    public static class SeqInputSplit implements InputSplit, Writable {

        public List<MatrixRange> ranges;

        public SeqInputSplit(List<MatrixRange> ranges) {
            this.ranges = ranges;
        }

        public List<MatrixRange> getRanges() {
            return ranges;
        }


        // InputSplit interface
        @Override public long getLength() throws IOException {
            return 0; // doesn't matter
        }

        @Override public String[] getLocations() throws IOException {
            return new String[] {}; // also doesn't matter.
        }

        // Writable interface.  Default constructor required for use with writable
        public SeqInputSplit() {
        }

        @Override public void write(DataOutput out) throws IOException {
            out.writeInt(ranges.size());
            for (MatrixRange mr : ranges) {
                mr.write(out);
            }
        }

        @Override public void readFields(DataInput in) throws IOException {
            int size = in.readInt();
            ranges = new ArrayList<MatrixRange>(size);
            for (int i=0; i < size; i++) {
                MatrixRange mr = new MatrixRange();
                mr.readFields(in);
                ranges.add(mr);
            }
        }
    }

    public static class SeqRecordReader implements RecordReader<MatrixRange,NullWritable> {

        SeqInputSplit split;
        int currentRange = 0;

        public SeqRecordReader(SeqInputSplit split) {
            this.split = split;
        }

        // RecordReader interface
        public boolean next(MatrixRange key, NullWritable value) throws IOException {
            if (currentRange >= split.ranges.size()) {
                return false;
            } else {
                MatrixRange range = split.ranges.get(currentRange);
                key.set(range.x, range.y, range.offset);
                currentRange++;
                return true;
            }
        }

        public MatrixRange createKey() { return new MatrixRange(); }

        public NullWritable createValue() { return NullWritable.get(); }

        public long getPos() throws IOException { return currentRange; }

        public void close() throws IOException { /* do nothing */ }

        public float getProgress() throws IOException {
            return currentRange / (float)split.ranges.size();
        }

    }
}
