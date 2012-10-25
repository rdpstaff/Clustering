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

import edu.msu.cme.rdp.hadoop.distance.mapred.keys.DistanceAndComparison;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * Sends DistancesAndComparison keys to the right partition based upon a previous
 * random sampling of distances.
 * @author farrisry
 */
public class DistancePartitioner implements Partitioner<DistanceAndComparison,NullWritable> {

    /**
     * The name of the distance samples file in the current working directory/DistributedCache
     */
    public static String SAMPLE_DISTANCES_FILE_NAME = "samples.txt";


    int partitions;
    final List<Integer> partitionLines = new ArrayList<Integer>();
    

    public int getPartition(DistanceAndComparison key, NullWritable value, int partitions) {
        if (partitions != this.partitions) {
            throw new RuntimeException("number of partitions has changed.  Cannot continue.");
        }

        for (int i=0; i < partitionLines.size(); i++) {
            if (key.distance < partitionLines.get(i)) {
                return i;
            }
        }
        return partitions - 1;

//        for (int i=0; i<partitions; i++) {
//            if (i >= partitionLines.size()) {
//                return i-1;
//            }
//            if (key.distance < partitionLines.get(i)) {
//                return i;
//            }
//        }
//        return partitions - 1; // last partition
    }


    public void configure(JobConf conf) {
        // load sample file and figure out the bins.

        partitions = conf.getInt("mapred.reduce.tasks", -1);
        if (partitions <= 0) {
            throw new RuntimeException("could not figure out the number of partitions");
        }

        // get the distance sampling file from the DistributedCache
        try {
            LocalFileSystem fs = FileSystem.getLocal(conf);
            Path samplePath = new Path(SAMPLE_DISTANCES_FILE_NAME);
            File sampleFile = fs.pathToFile(samplePath);

            // count the number of distance samples
            BufferedReader reader = new BufferedReader(new FileReader(sampleFile));
            String line;

            int count = 0;
            while ((line = reader.readLine()) != null) {
                count++;
            }

            // find the distances at which the comparisons fall into equally sized bins
            reader = new BufferedReader(new FileReader(sampleFile));

            int perBin = new Double(Math.ceil(count / (double)partitions)).intValue();

            count = 0;
            int binCount = 0;
            while ((line = reader.readLine()) != null) {
                count++;
                binCount++;
                if (binCount > perBin) {
                    String[] tokens = line.split("\t");
                    int dist = Integer.parseInt(tokens[0]);
                    partitionLines.add(dist);
                    binCount = 0;
                }
            }
            // it is possible partitionLines.size() = partitions + 1, but the logic of getPartition
            // will ignore that extra bin.

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        System.out.println("PARTITIONS: " + partitions);
        for (int dist : partitionLines) {
            System.out.println("LINE: " + dist);
        }
    }

}
