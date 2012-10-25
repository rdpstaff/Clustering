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

import edu.msu.cme.rdp.hadoop.distance.mapred.keys.DistanceAndComparison;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * Sends DistancesAndComparison keys to the right partition based upon a previous
 * random sampling of distances.
 * @author farrisry
 */
public class DistancePartitionerOneOff implements Partitioner<DistanceAndComparison,NullWritable> {

    public int getPartition(DistanceAndComparison key, NullWritable value, int partitions) {
        return partitions - 1;
    }


    public void configure(JobConf conf) {
    }

}
