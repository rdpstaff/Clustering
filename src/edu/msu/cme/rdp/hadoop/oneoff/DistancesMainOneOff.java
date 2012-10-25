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
package edu.msu.cme.rdp.hadoop.oneoff;

import edu.msu.cme.rdp.alignment.pairwise.rna.DistanceModel;
import edu.msu.cme.rdp.alignment.pairwise.rna.UncorrectedDistanceModel;
import edu.msu.cme.rdp.hadoop.distance.mapred.ByteSeqInputFormat;
import edu.msu.cme.rdp.hadoop.distance.mapred.keys.Comparison;
import edu.msu.cme.rdp.hadoop.distance.mapred.keys.DistanceAndComparison;
import edu.msu.cme.rdp.hadoop.distance.mapred.keys.IntDistance;
import edu.msu.cme.rdp.hadoop.distance.mapred.DistanceAndComparisonMapper;
import edu.msu.cme.rdp.hadoop.distance.mapred.DistancePartitioner;
import edu.msu.cme.rdp.hadoop.distance.mapred.DistanceReducer;
import edu.msu.cme.rdp.hadoop.utils.AlignedIntSeqStore;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author farrisry
 */
public class DistancesMainOneOff extends Configured implements Tool {

    public static void main(String... args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DistancesMainOneOff(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        float cutoff = .1f;
        int overlapLimit = 1;

        Path byteSeqPath = new Path("/user/fishjord/hmp_dacc/hmp_dacc_seqs.bin");
        URI seqFileCacheURI = new URI("/user/fishjord/hmp_dacc/hmp_dacc_seqs.bin#" + DistanceAndComparisonMapper.SEQ_FILE_NAME);

        Path outputDir = new Path("/user/fishjord/hmp_dacc/matrix"); // fine as a plain Path

        JobConf conf = new JobConf(getConf(), this.getClass());
        FileSystem fs = outputDir.getFileSystem(conf);

        int reducers = 6;

        // get the number of sequences in the byteSeqFile
        FSDataInputStream seqFileStream = null;
        long numSeqs = -1;
        try {
            seqFileStream = fs.open(byteSeqPath);
            numSeqs = AlignedIntSeqStore.numberOfSeqs(seqFileStream);
        } finally {
            if (seqFileStream != null) {
                seqFileStream.close();
            }
        }

	conf.setInt("mapred.reduce.tasks", reducers);
        conf.setInt("mapred.map.tasks", 18);

        // set values required by the InputFormat and Mapper
        conf.setLong(ByteSeqInputFormat.NUM_SEQS_KEY, numSeqs);
        conf.setInt(DistanceAndComparisonMapper.DISTANCE_CUTOFF_KEY, IntDistance.scale(cutoff));
        conf.setInt(DistanceAndComparisonMapper.DISTANCE_OVERLAP_LIMIT_KEY, overlapLimit);

        Class model = UncorrectedDistanceModel.class;
        conf.setClass(DistanceAndComparisonMapper.DISTANCE_MODEL_KEY, model, DistanceModel.class);
        System.out.println("Using " + model.getCanonicalName() + " model");
        System.out.println("Using " + reducers + " reducers");

        System.out.println("Conf: " + conf);


        // load the samplesFile and byteSeqFile into the distributed cache
        URI samplesFileURI = new URI("/user/fishjord/hmp_dacc/sampling/part-00000#" + DistancePartitioner.SAMPLE_DISTANCES_FILE_NAME);
        DistributedCache.addCacheFile(samplesFileURI, conf);
        DistributedCache.addCacheFile(seqFileCacheURI, conf);
        DistributedCache.createSymlink(conf);

        // configure the job
        conf.setMapperClass(DistanceAndComparisonMapper.class);
        conf.setReducerClass(DistanceReducer.class);
        conf.setPartitionerClass(DistancePartitioner.class);

        conf.setInputFormat(ByteSeqInputFormatOneOff.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(conf, outputDir);

        conf.setMapOutputKeyClass(DistanceAndComparison.class);
        conf.setMapOutputValueClass(NullWritable.class);
        // the setOutputKeyComparatorClass is mislabeled and really should be called setMapOutputKeyComparatorClass
        conf.setOutputKeyComparatorClass(DistanceAndComparison.GroupingComparator.class);

        conf.setOutputValueClass(Comparison.class);
        conf.setOutputKeyClass(IntDistance.class);

        conf.setJarByClass(DistanceAndComparisonMapper.class);


        JobClient.runJob(conf);

        return 0;
    }
}
