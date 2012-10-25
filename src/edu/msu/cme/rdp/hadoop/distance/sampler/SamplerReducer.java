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

import edu.msu.cme.rdp.hadoop.distance.mapred.keys.DistanceAndComparison;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Converts the DistanceAndComparison key to text before outputing.
 * 
 * @author farrisry
 */
public class SamplerReducer implements Reducer<DistanceAndComparison,NullWritable,Text,NullWritable> {

    public void reduce(DistanceAndComparison key, Iterator<NullWritable> value, OutputCollector<Text, NullWritable> out, Reporter report) throws IOException {
        out.collect(new Text(key.distance + "\t" + key.first + "\t" + key.second), NullWritable.get());
    }

    public void configure(JobConf conf) {}

    public void close() throws IOException { }

}
