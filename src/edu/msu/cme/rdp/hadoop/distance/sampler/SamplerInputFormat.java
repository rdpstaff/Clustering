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

package edu.msu.cme.rdp.hadoop.distance.sampler;

import edu.msu.cme.rdp.hadoop.distance.mapred.ByteSeqInputFormat;
import edu.msu.cme.rdp.hadoop.distance.mapred.keys.MatrixRange;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

/**
 * Provides (somewhat) random input to the mappers to generate a set of sample
 * distances which can be used to partition the full matrix evenly.
 *
 * This selects random MatrixRanges to run, not random comparisons, so it may be
 * advisable to reduce the RECORD_SIZE_KEY and increase the SAMPLE_SIZE_KEY to
 * obtain a better sampling.
 * @author farrisry
 */
public class SamplerInputFormat extends ByteSeqInputFormat {

    /**
     * Number of random matrix ranges to select for distance sampling.
     */
    public static String SAMPLE_SIZE_KEY = "rdp.distance.sampleSize";

    
    @Override
    public InputSplit[] getSplits(JobConf conf, int suggestedSplits) throws IOException {
        SeqInputSplit[] splits =  (SeqInputSplit[])super.getSplits(conf, suggestedSplits);

        long rangesToSample = conf.getLong(SAMPLE_SIZE_KEY, 1000);

        List<MatrixRange> matrixRanges = new ArrayList<MatrixRange>();
        for (SeqInputSplit split : splits) {
            matrixRanges.addAll(split.getRanges());
        }

        if (matrixRanges.size() <= rangesToSample) {
            return splits;
        }

        Set<MatrixRange> randomRanges = new HashSet<MatrixRange>();

        Random rand = new Random();
        while (randomRanges.size() < rangesToSample) {
            randomRanges.add(matrixRanges.get(rand.nextInt(matrixRanges.size())));
        }
        
        List<SeqInputSplit> randomSplits = new ArrayList<SeqInputSplit>();
        for (MatrixRange range : randomRanges) {
            List<MatrixRange> tmp = new ArrayList<MatrixRange>();
            tmp.add(range);
            randomSplits.add(new SeqInputSplit(tmp));
        }

        return randomSplits.toArray(new SeqInputSplit[randomSplits.size()]);
    }

}
