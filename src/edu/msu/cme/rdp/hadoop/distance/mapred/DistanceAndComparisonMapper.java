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

package edu.msu.cme.rdp.hadoop.distance.mapred;

import edu.msu.cme.rdp.alignment.pairwise.rna.DistanceModel;
import edu.msu.cme.rdp.alignment.pairwise.rna.OverlapCheckFailedException;
import edu.msu.cme.rdp.alignment.pairwise.rna.UncorrectedDistanceModel;
import edu.msu.cme.rdp.hadoop.distance.mapred.keys.MatrixRange;
import edu.msu.cme.rdp.hadoop.distance.mapred.keys.IntDistance;
import edu.msu.cme.rdp.hadoop.distance.mapred.keys.DistanceAndComparison;
import edu.msu.cme.rdp.hadoop.utils.AlignedIntSeqStore;
import edu.msu.cme.rdp.hadoop.utils.IntSeq;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


/**
 * Takes a MatrixRange as a key and computes all the distances within that range,
 * passing them on as DistanceAndComparison keys.
 *
 * Requires:
 *   AlignedIntByteSeq-formatted sequence file placed in the current working directory
 *    by the DistributedCache.
 *   Distance cutoff over which distances are thrown out.
 *
 * Optional:
 *   Specify a distance model to be used to calcuate the distances.  Default is Uncorrected.
 *   Specify a minimum overlap limit for sequence comparisons under which the comparison is thrown out.  Default is 1 comparable base.
 *
 * @author farrisry
 */
public class DistanceAndComparisonMapper implements Mapper<MatrixRange,NullWritable,DistanceAndComparison,NullWritable> {

    private static final Logger noOverlapLogger = Logger.getLogger("nooverlap");

    /**
     * The name of the sequence file in the current working directory/DistributedCache
     */
    public static String SEQ_FILE_NAME = "sequenceFile.bin";

    /**
     * JobConf key for the Distance model.  Must be the canonical name for one of
     * the RDP distance model classes in edu.msu.cme.rdp.pyro.distance.
     * Defaults to UncorrectedDistanceModel.
     */
    public static String DISTANCE_MODEL_KEY = "rdp.distance.model";

    /**
     * JobConf key for the distance cutoff at which distances are thrown out.
     */
    public static String DISTANCE_CUTOFF_KEY = "rdp.distance.max";
    /**
     * JobConf key for the minimum number of bases that must overlap for a distance to be used.
     * Comparisons that fail this check are logged to the system logger.
     * Ideally, all sequences should  contain data from the same location on the molecule
     * and have passed some quality control steps to weed out short sequences, making
     * this check redundant.  The default value is 1 position.
     */
    public static String DISTANCE_OVERLAP_LIMIT_KEY = "rdp.distance.overlap";


    DistanceModel model;
    int cutoff;
    int overlapLimit;
    File byteSeqFile;

    private DistanceAndComparison outKey = new DistanceAndComparison();
    private NullWritable nullValue = NullWritable.get();


    public void map(MatrixRange key, NullWritable value, OutputCollector<DistanceAndComparison, NullWritable> out, Reporter report) throws IOException {
        noOverlapLogger.log(Level.INFO, "Output class: {0}", out.getClass());

        long numOfSeqs = AlignedIntSeqStore.numberOfSeqs(byteSeqFile);
        List<IntSeq> xSeqs = AlignedIntSeqStore.fromFile(byteSeqFile, key.x, key.offset);
        List<IntSeq> ySeqs = AlignedIntSeqStore.fromFile(byteSeqFile, key.y, key.offset);

        for (long x = key.x; x < (key.x + key.offset) && x < numOfSeqs; x++) {
            int i = (int) (x - key.x);
            IntSeq xSeq = xSeqs.get(i);
            for (long y = key.y; y < (key.y + key.offset) && y < numOfSeqs; y++) {
                if (y <= x) {
                    continue; // skip the right upper portion of the matrix.
                }
                int j = (int) (y - key.y);
                IntSeq ySeq = ySeqs.get(j);

                try {
                    int score = IntDistance.scale(model.getDistance(xSeq.getBytes(), ySeq.getBytes(), overlapLimit));

                    if (score <= cutoff) {

                        outKey.setDistance(score);
                        outKey.setFirst(xSeq.getId());
                        outKey.setSecond(ySeq.getId());

                        out.collect(outKey, nullValue);
                    }
                } catch (OverlapCheckFailedException ex) {
                    noOverlapLogger.log(Level.WARNING, "NoOverlap {0} {1} {2}", new Object[]{overlapLimit, x, y});
                }
            }
        }
    }

    public void configure(JobConf conf) {
        cutoff = conf.getInt(DISTANCE_CUTOFF_KEY, -1);
        if (cutoff < 0) {
            throw new RuntimeException("No distance cutoff found in JobConf.");
        }

        overlapLimit = conf.getInt(DISTANCE_OVERLAP_LIMIT_KEY, 1);

        // instantiate and instance of the distance model specified.
        try {
            model = (DistanceModel) conf.getClass(DISTANCE_MODEL_KEY, UncorrectedDistanceModel.class).newInstance();
        } catch (InstantiationException ex) {
            throw new RuntimeException(ex);
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }

        // get the sequence file which the DistributedCache has placed in the working dir.
        try {
            LocalFileSystem fs = FileSystem.getLocal(conf);
            Path seqPath = new Path(SEQ_FILE_NAME);
            byteSeqFile = fs.pathToFile(seqPath);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void close() throws IOException {
        // nothing to do
    }

}