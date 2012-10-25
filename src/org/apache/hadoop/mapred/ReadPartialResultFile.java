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
package org.apache.hadoop.mapred;

import edu.msu.cme.rdp.hadoop.distance.mapred.ByteSeqInputFormat;
import edu.msu.cme.rdp.hadoop.distance.mapred.keys.DistanceAndComparison;
import edu.msu.cme.rdp.hadoop.distance.mapred.keys.MatrixRange;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * @author Jordan Fish <fishjord at msu.edu>
 */
public class ReadPartialResultFile {

    private static void guessSplitRanges() throws IOException {

        long numSeqs = 2334222;
        int inMemSeqs = 2000;
        long rangesPerSplit = 100;
        // figure out number of ranges per split to use
        long numRanges = (numSeqs / inMemSeqs) * (numSeqs / inMemSeqs) / 2;

        List<ByteSeqInputFormat.SeqInputSplit> splits = new ArrayList<ByteSeqInputFormat.SeqInputSplit>();
        ByteSeqInputFormat.SeqInputSplit split = new ByteSeqInputFormat.SeqInputSplit(new ArrayList<MatrixRange>());
        splits.add(split);
        for (long x=0; x < numSeqs; x += inMemSeqs) {
            for (long y=x; y < numSeqs; y += inMemSeqs) {
                MatrixRange mr = new MatrixRange();
                mr.set(x, y, inMemSeqs);
                if (split.ranges.size() >= rangesPerSplit) {
                    split = new ByteSeqInputFormat.SeqInputSplit(new ArrayList<MatrixRange>());
                    splits.add(split);
                }
                split.ranges.add(mr);
            }
        }

        int i = 0;

        PrintStream out = new PrintStream("/home/fishjord/tasks_to_splits_detailed.txt");
        for(ByteSeqInputFormat.SeqInputSplit s : splits) {
            Collections.sort(s.ranges);
            int j = 0;
            for(MatrixRange mr : s.ranges) {
                out.println(i + "\t" + j + "\t" + mr.x + "\t" + mr.y + "\t" + mr.offset);
                j++;
            }
            i++;
        }
        out.close();

        i = 0;
        out = new PrintStream("/home/fishjord/tasks_to_splits.txt");
        for(ByteSeqInputFormat.SeqInputSplit s : splits) {
            Collections.sort(s.ranges);
            long minX = Long.MAX_VALUE;
            long minY = Long.MAX_VALUE;
            long maxX = 0;
            long maxY = 0;
            for(MatrixRange mr : s.ranges) {
                long x = mr.x + mr.offset;
                long y = mr.y + mr.offset;

                if(mr.x < minX) {
                    minX = mr.x;
                }
                if(mr.y < minY) {
                    minY = mr.y;
                }

                if(x > maxX) {
                    maxX = x;
                }
                if(y > maxY) {
                    maxY = y;
                }
            }
            out.println(i + "\t" + minX + "\t" + maxX + "\t" + minY + "\t" + maxY);
            i++;
        }
        out.close();
    }

    public static void main(String[] args) throws Exception {
        guessSplitRanges();
        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (args.length != 1) {
            System.err.println("USAGE: ReadPartialResultFile <job_dir>");
            System.exit(1);
        }

        File jobDir = new File(args[0]);
        String jobName = jobDir.getName();
        if (!jobName.startsWith("job_")) {
            throw new IOException("Expected job dir name to start with 'job_'");
        }

        FileSystem fs = FileSystem.getLocal(conf);

        DataInputBuffer keyBuf = new DataInputBuffer();
        DataInputBuffer valBuf = new DataInputBuffer();

        System.out.println("path\tattempt\tnum_dists\tsorted?\tminX\tmaxX\tminY\tmaxY\tavg_dist");

        for (File attemptDir : jobDir.listFiles()) {
            if (!attemptDir.isDirectory() || !attemptDir.getName().startsWith("attempt")) {
                continue;
            }

            String attemptNum = attemptDir.getName().split("_")[4];

            int numDirs = 0;
            for (File f : attemptDir.listFiles()) {
                if (f.isDirectory()) {
                    numDirs++;
                }
            }

            if (numDirs != 1) {
                System.err.println(attemptDir + " does not have exactly one sub directory");
                continue;
            }

            File fileOut = new File(attemptDir, "output/file.out.gz");

            if (!fileOut.exists()) {
                System.err.println("Couldn't find output directory in " + attemptDir);
                continue;
            }

            Path path = new Path(fileOut.getAbsolutePath());
            IFile.Reader<DistanceAndComparison, NullWritable> reader = new IFile.Reader<DistanceAndComparison, NullWritable>(conf, fs, path, new GzipCodec());

            int numDists = 0;
            int minX = Integer.MAX_VALUE;
            int maxX = 0;
            int minY = Integer.MAX_VALUE;
            int maxY = 0;
            long totalDist = 0;
            DistanceAndComparison ds = new DistanceAndComparison();

            int lastDist = -1;
            boolean sorted = true;

            while (reader.next(keyBuf, valBuf)) {
                ds.readFields(keyBuf);
                numDists++;

                if (ds.first < minX) {
                    minX = ds.first;
                }

                if (ds.first > maxX) {
                    maxX = ds.first;
                }

                if (ds.second < minY) {
                    minY = ds.second;
                }

                if (ds.second > minY) {
                    minY = ds.second;
                }

                if (lastDist > ds.distance) {
                    sorted = false;
                }

                lastDist = ds.distance;
                totalDist += ds.distance;
            }

            System.out.println(fileOut + "\t" + attemptNum + "\t" + numDists + "\t" + sorted + "\t" + minX + "\t" + maxX + "\t" + minY + "\t" + maxY + "\t" + ((float) totalDist / numDists));
            reader.close();
        }
    }
}
