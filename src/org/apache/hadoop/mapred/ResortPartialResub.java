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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * @author Jordan Fish <fishjord at msu.edu>
 */
public class ResortPartialResub {

    private static class ExpectedRange {

        final int minX, maxX;
        final int minY, maxY;
        final int taskid;

        IFile.Writer<DistanceAndComparison, NullWritable> out;

        public ExpectedRange(Configuration conf, FileSystem fs, String[] lexemes) throws IOException {
            taskid = Integer.valueOf(lexemes[0]);
            minX = Integer.valueOf(lexemes[1]);
            maxX = Integer.valueOf(lexemes[2]);
            minY = Integer.valueOf(lexemes[3]);
            maxY = Integer.valueOf(lexemes[4]);

            new File(lexemes[0]).mkdir();
            out = new IFile.Writer<DistanceAndComparison, NullWritable>(conf, fs, new Path(taskid + "/file.out"), DistanceAndComparison.class, NullWritable.class, null);
        }
    }

    private static List<ExpectedRange> loadRanges(Configuration conf, FileSystem fs, File f) throws IOException {
        List<ExpectedRange> ret = new ArrayList();

        BufferedReader reader = new BufferedReader(new FileReader(f));
        String line;
        while((line = reader.readLine()) != null) {
            String[] lexemes = line.split("\\s+");
            if(lexemes.length != 5) {
                continue;
            }

            ret.add(new ExpectedRange(conf, fs, lexemes));
        }

        return ret;
    }

    public static void main(String[] args) throws Exception {
        //guessSplitRanges();
        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (args.length != 2) {
            System.err.println("USAGE: ReadPartialResultFile <job_dir> <expected_splits>");
            System.exit(1);
        }

        FileSystem fs = FileSystem.getLocal(conf);
        List<ExpectedRange> ranges = loadRanges(conf, fs, new File(args[1]));
        System.err.println("Loaded ranges");

        CombinedIntermediateSortedReader reader = CombinedIntermediateSortedReader.fromDirectory(conf, new File(args[0]));
        System.err.println("Created reader");
        DistanceAndComparison dist;
        NullWritable nw = NullWritable.get();

        while((dist = reader.next()) != null) {
            boolean found = false;
            for(ExpectedRange range : ranges) {
                if(dist.first >= range.minX && dist.first < range.maxX &&
                        dist.second >= range.minY && dist.second < range.maxY) {
                    range.out.append(dist, nw);
                    found = true;
                    break;
                }
            }

            if(!found) {
                System.err.println("Couldn't map " + dist.first + " " + dist.second);
            }
        }

        for(ExpectedRange range : ranges) {
            range.out.close();
        }
    }
}
