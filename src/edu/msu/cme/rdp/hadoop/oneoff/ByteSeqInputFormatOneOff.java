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

package edu.msu.cme.rdp.hadoop.oneoff;

import edu.msu.cme.rdp.hadoop.distance.mapred.ByteSeqInputFormat;
import edu.msu.cme.rdp.hadoop.distance.mapred.keys.MatrixRange;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

/**
 * Breaks the distance matrix up into subsections for the mappers to work on.
 *
 * Each Mapper has access to the entire sequence file via the DistributedCache.
 * Each key fed to the mapper is a subsection of the distance matrix to compute.
 *
 * @author farrisry
 */
public class ByteSeqInputFormatOneOff extends ByteSeqInputFormat {

    private InputSplit[] splits = null;

    public ByteSeqInputFormatOneOff() {

    }

    public static InputSplit[] loadRanges(File f) throws IOException {
        System.err.println("Loading ranges");
        Map<Integer, ByteSeqInputFormat.SeqInputSplit> taskIds = new HashMap();

        BufferedReader reader = new BufferedReader(new FileReader(f));
        String line;
        while((line = reader.readLine()) != null) {
            taskIds.put(Integer.valueOf(line), new ByteSeqInputFormat.SeqInputSplit());
        }
        reader.close();

        reader = new BufferedReader(new FileReader("/home/fishjord/tmp/tasks_to_splits_detailed.txt"));
        while((line = reader.readLine()) != null) {
            String[] lexemes = line.trim().split("\\s+");
            int tid = Integer.valueOf(lexemes[0]);

            ByteSeqInputFormat.SeqInputSplit split = taskIds.get(tid);
            if(split != null) {
                if(split.ranges == null) {
                    split.ranges = new ArrayList();
                }
                MatrixRange mr = new MatrixRange();
                mr.x = Integer.valueOf(lexemes[2]);
                mr.y = Integer.valueOf(lexemes[3]);
                mr.offset = Integer.valueOf(lexemes[4]);
                split.ranges.add(mr);
            }

        }

        reader.close();


        List<InputSplit> ranges = new ArrayList();
        for(Integer tid : taskIds.keySet()) {
            ranges.add(taskIds.get(tid));
        }

        System.err.println("Loaded " + ranges.size() + " ranges to recomputer");
        return ranges.toArray(new InputSplit[ranges.size()]);
    }

    /**
     * Breaks the distance matrix generation up into smaller subsections.
     *
     * @param conf JobConf must have NUM_SEQS_KEY value set to the number of sequences.
     * @param suggestedSplits ignored unless the matrix is small relative to the suggested size.
     * @return
     * @throws IOException
     */
    @Override
    public InputSplit[] getSplits(JobConf conf, int suggestedSplits) throws IOException {
        if(splits == null) {
            splits = loadRanges(new File("/home/fishjord/tmp/resub_taskids.txt"));
        }
        return splits;
    }
}
