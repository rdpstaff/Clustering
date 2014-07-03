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
package edu.msu.cme.pyro.cluster.io;

import edu.msu.cme.pyro.cluster.dist.DistanceCalculator;
import edu.msu.cme.pyro.cluster.utils.AbstractClusterFactory;
import edu.msu.cme.pyro.cluster.utils.Cluster;
import edu.msu.cme.pyro.derep.SampleMapping;
import edu.msu.cme.pyro.derep.IdMapping;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author fishjord
 */
public class ClusterFileOutput implements ClusterOutput {

    final IdMapping<Integer> intToIdMap;
    final SampleMapping<String> idToSampleMap;
    final PrintStream out;

    final DecimalFormat format = new DecimalFormat("0.0###");

    public ClusterFileOutput(IdMapping<Integer> idMapping, SampleMapping<String> sampleMapping, PrintStream out) {
        this.intToIdMap = idMapping;
        this.idToSampleMap = sampleMapping;
        this.out = out;
        Map<String, Integer> sampleCountMap = new LinkedHashMap();
        for(Integer id : idMapping.getAll()) {
            for(String seqid : idMapping.getIds(id)) {
                String sample = sampleMapping.getSampleById(seqid);
                if(!sampleCountMap.containsKey(sample)) {
                    sampleCountMap.put(sample, 0);
                }
                sampleCountMap.put(sample, sampleCountMap.get(sample) + 1);
            }
        }

        out.print("File(s):\t");
        for (String file : sampleMapping.getSampleList()) {
            out.print(file + " ");
        }
        out.println();
        out.print("Sequences:\t");

        for (String file : sampleMapping.getSampleList()) {
            out.print(sampleCountMap.get(file) + " ");
        }
        out.println();
        out.println();
    }

    public void printClusters(AbstractClusterFactory factory, int step) {
        List<Integer> unseenIds = new LinkedList<Integer>();
        Set<Cluster> clusters = new LinkedHashSet();

        for (Integer seqid : intToIdMap.getAll()) {
            Cluster c = factory.getCluster(seqid);
            if (c == null) {
                unseenIds.add(seqid);
            } else {
                clusters.add(c);
            }
        }

        double actualStep = step / (double)DistanceCalculator.MULTIPLIER;
        out.println("distance cutoff:\t" + format.format(actualStep));
        out.println("Total Clusters:\t" + (clusters.size() + unseenIds.size()));
        int clusterCount = 1;
        for (Cluster c : clusters) {
            Set<Integer> sids = factory.getSeqsInCluster(c);
            Set<String> ids = new LinkedHashSet();
            for (int sid : sids) {
                ids.addAll(intToIdMap.getIds(sid));
            }

            for (String sample : idToSampleMap.getSampleList()) {

                Set<String> fileIds = new LinkedHashSet<String>(ids);
                fileIds.retainAll(idToSampleMap.getIdsBySample(sample));

                if (fileIds.size() > 0) {
                    StringBuilder buf = new StringBuilder();
                    buf.append(clusterCount).append("\t");
                    buf.append(sample).append("\t");
                    buf.append(fileIds.size()).append("\t");
                    for (String id : fileIds) {
                        buf.append(id).append(" ");
                    }
                    out.println(buf.toString());
                }
            }
            clusterCount++;
        }

        for (int sid : unseenIds) {
            Set<String> ids = new LinkedHashSet(intToIdMap.getIds(sid));

            for (String sample : idToSampleMap.getSampleList()) {

                Set<String> fileIds = new LinkedHashSet<String>(ids);
                fileIds.retainAll(idToSampleMap.getIdsBySample(sample));

                if (fileIds.size() > 0) {
                    StringBuilder buf = new StringBuilder();
                    buf.append(clusterCount).append("\t");
                    buf.append(sample).append("\t");
                    buf.append(fileIds.size()).append("\t");
                    for (String id : fileIds) {
                        buf.append(id).append(" ");
                    }
                    out.println(buf.toString());
                }
            }
            clusterCount++;
        }
        out.println();
        out.flush();
    }

    public void close() {
        out.close();
    }
}
