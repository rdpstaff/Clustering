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

import edu.msu.cme.rdp.hadoop.distance.mapred.keys.DistanceAndComparison;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.GzipCodec;

/**
 *
 * @author Jordan Fish <fishjord at msu.edu>
 */
public class CombinedIntermediateSortedReader {

    private static class Holder implements Comparable<Holder> {

        DistanceAndComparison dist;
        IFile.Reader<DistanceAndComparison, NullWritable> reader;

        public Holder(DistanceAndComparison dist, IFile.Reader<DistanceAndComparison, NullWritable> reader) {
            this.dist = dist;
            this.reader = reader;
        }

        public int compareTo(Holder t) {
            return dist.compareTo(t.dist);
        }
    }
    private PriorityQueue<Holder> pq = new PriorityQueue();
    private DataInputBuffer keyBuf = new DataInputBuffer();
    private DataInputBuffer valBuf = new DataInputBuffer();

    public CombinedIntermediateSortedReader(List<IFile.Reader<DistanceAndComparison, NullWritable>> sortedSplits) throws IOException {
        for (IFile.Reader<DistanceAndComparison, NullWritable> reader : sortedSplits) {
            Holder h = readFrom(reader);
            if (h != null) {
                pq.offer(h);
            }
        }
    }

    public DistanceAndComparison next() throws IOException {
        if (pq.isEmpty()) {
            return null;
        }

        Holder h = pq.poll();
        Holder add = readFrom(h.reader);

        if (add != null) {
            pq.offer(add);
        }

        return h.dist;
    }

    private Holder readFrom(IFile.Reader<DistanceAndComparison, NullWritable> reader) throws IOException {
        try {
            if (!reader.next(keyBuf, valBuf)) {
                reader.close();
                return null;
            }

            DistanceAndComparison ret = new DistanceAndComparison();
            ret.readFields(keyBuf);

            return new Holder(ret, reader);
        } catch (IOException e) {
            throw new IOException("Failed to read from " + reader);
        }
    }

    public void close() throws IOException {
        while (!pq.isEmpty()) {
            Holder h = pq.poll();
            h.reader.close();
        }
    }

    public static CombinedIntermediateSortedReader fromDirectory(Configuration conf, File... directory) throws IOException {
        List<IFile.Reader<DistanceAndComparison, NullWritable>> readers = new ArrayList();
        GzipCodec gzip = new GzipCodec();
        gzip.setConf(conf);

        FileSystem fs = FileSystem.getLocal(conf);

        for (File d : directory) {
            fromDirectory(conf, fs, gzip, d, readers);
        }

        return new CombinedIntermediateSortedReader(readers);
    }

    private static void fromDirectory(Configuration conf, FileSystem fs, GzipCodec codec, File d, List<IFile.Reader<DistanceAndComparison, NullWritable>> readers) throws IOException {
        for (File f : d.listFiles()) {
            if (f.isDirectory()) {
                fromDirectory(conf, fs, codec, f, readers);
            } else if (f.isFile()) {
                try {
                    if (f.getName().equals("file.out.gz")) {
                        readers.add(new IFile.Reader<DistanceAndComparison, NullWritable>(conf, fs, new Path(f.getAbsolutePath()), codec));
                    } else if (f.getName().equals("file.out")) {
                        readers.add(new IFile.Reader<DistanceAndComparison, NullWritable>(conf, fs, new Path(f.getAbsolutePath()), null));
                    }
                } catch (IOException e) {
                    throw new IOException("Failed to open " + f.getAbsolutePath());
                }
            }
        }
    }
}
