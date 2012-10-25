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
package edu.msu.cme.rdp.hadoop.distance;

import edu.msu.cme.rdp.alignment.pairwise.rna.DistanceModel;
import edu.msu.cme.rdp.alignment.pairwise.rna.IdentityDistanceModel;
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
public class DistancesMain extends Configured implements Tool {

    public static void main(String... args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DistancesMain(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        if (args.length != 6 && args.length != 7) {
            System.out.println("usage: <distance_cutoff> <seq_overlap_limit> <distance_samples_file> <byte_seq_file> <output_dir> <num_reducers> [identity distance model]");
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        float cutoff = Float.parseFloat(args[0]);
        int overlapLimit = Integer.parseInt(args[1]);
        // the #fileName causes the DistributedCache to place a symlink in the working dir.
        URI samplesFileURI = new URI(args[2] + "#" + DistancePartitioner.SAMPLE_DISTANCES_FILE_NAME);

        Path byteSeqPath = new Path(args[3]);
        URI seqFileCacheURI = new URI(args[3] + "#" + DistanceAndComparisonMapper.SEQ_FILE_NAME);

        Path outputDir = new Path(args[4]); // fine as a plain Path

        JobConf conf = new JobConf(getConf(), this.getClass());
        FileSystem fs = outputDir.getFileSystem(conf);

        int reducers = Integer.valueOf(args[5]);

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
        if (args.length == 7 && args[6].equals("true")) {
            model = IdentityDistanceModel.class;
        }
        conf.setClass(DistanceAndComparisonMapper.DISTANCE_MODEL_KEY, model, DistanceModel.class);
        System.out.println("Using " + model.getCanonicalName() + " model");
        System.out.println("Using " + reducers + " reducers");

        System.out.println("Conf: " + conf);


        // load the samplesFile and byteSeqFile into the distributed cache
        DistributedCache.addCacheFile(samplesFileURI, conf);
        DistributedCache.addCacheFile(seqFileCacheURI, conf);
        DistributedCache.createSymlink(conf);

        // configure the job
        conf.setMapperClass(DistanceAndComparisonMapper.class);
        conf.setReducerClass(DistanceReducer.class);
        conf.setPartitionerClass(DistancePartitioner.class);

        conf.setInputFormat(ByteSeqInputFormat.class);
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
