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

package edu.msu.cme.pyro.cluster.utils;

import org.apache.commons.io.output.NullOutputStream;
import edu.msu.cme.pyro.cluster.dist.ThickEdge;
import edu.msu.cme.pyro.cluster.dist.ThinEdge;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author fishjord
 */
public class ClusterFactory implements AbstractClusterFactory {
    private DataOutputStream mergesStream = null;
    private int clustCount = 0;
    private Map<Integer, Cluster> seqToClusterMap = new LinkedHashMap();
    protected ClusterEdges edges = new ClusterEdges();
    private Map<Cluster, Set<Integer>> clustersToSeqs = new LinkedHashMap();

    public ClusterFactory() {
        mergesStream = new DataOutputStream(new NullOutputStream());
    }

    public ClusterFactory(File mergesFile) throws IOException {
        mergesStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(mergesFile)));
    }

    public Cluster getCluster(int seq) {
        return seqToClusterMap.get(seq);
    }

    public Cluster createSingleton(int seq) {
        Cluster c = new Cluster(clustCount++, 1);

        try {
            mergesStream.writeBoolean(true);
            mergesStream.writeInt(c.getId());
            mergesStream.writeInt(seq);
        } catch(IOException ignore) {}

        clustersToSeqs.put(c, new LinkedHashSet(Arrays.asList(seq)));
        seqToClusterMap.put(seq, c);
        return c;
    }

    public Set<Integer> getSeqsInCluster(Cluster c) {
        return clustersToSeqs.get(c);
    }

    public int getClustCount() {
        return new LinkedHashSet(seqToClusterMap.values()).size();
    }

    public int getEdgeCount() {
        return edges.size();
    }

    public ThickEdge createThickEdge(Cluster ci, Cluster cj, ThinEdge initEdge) {
        ThickEdge ret =  new ThickEdge(ci, cj, initEdge.getDist(), 1);
        edges.put(ci, cj, ret);
        return ret;
    }

    public ThickEdge getThickEdge(Cluster ci, Cluster cj) {
        return edges.get(ci, cj);
    }

    public Collection<ThickEdge> getThickEdges(Cluster c) {
        return edges.getEdges(c);
    }

    public Collection<Cluster> getClusters() {
        return new LinkedHashSet(seqToClusterMap.values());
    }

    public Cluster mergeCluster(Cluster ci, Cluster cj, int mergeDist) {
        Cluster ck = new Cluster(clustCount++, ci.getNumberOfSeqs() + cj.getNumberOfSeqs());

        try {
            mergesStream.writeBoolean(false);
            mergesStream.writeInt(ci.getId());
            mergesStream.writeInt(cj.getId());
            mergesStream.writeInt(ck.getId());
            mergesStream.writeInt(mergeDist);
        } catch(IOException ignore) {

        }

        Set<Integer> ciSeqs = clustersToSeqs.remove(ci);
        Set<Integer> cjSeqs = clustersToSeqs.remove(cj);

        Set<Integer> ckSeqs = new LinkedHashSet();
        ckSeqs.addAll(ciSeqs);
        ckSeqs.addAll(cjSeqs);
        clustersToSeqs.put(ck, ckSeqs);

        // update the seq id mappings
        for (Integer sid : ciSeqs) {
            seqToClusterMap.put(sid, ck);
        }

        for (Integer sid : cjSeqs) {
            seqToClusterMap.put(sid, ck);
        }

        edges.remove(ci, cj);
        Set<Cluster> existingEdgesI = edges.get(ci);
        Set<Cluster> existingEdgesJ = edges.get(cj);
        //System.out.println("Merging ci=" + ci.getSeqs() + " cj=" + cj.getSeqs() + " ck=" + ck.getSeqs());

        for (Cluster cn : existingEdgesI) {
            ThickEdge edge = edges.remove(ci, cn);
            long dist = edge.getSeenDistances();
            long seenEdges = edge.getSeenEdges();

            if (existingEdgesJ.contains(cn)) {
                // if Cn has edges with both Ci and Cj, then merge the edges.
                ThickEdge edgeJ = edges.remove(cj, cn);
                dist += edgeJ.getSeenDistances();
                seenEdges += edgeJ.getSeenEdges();

                existingEdgesJ.remove(cn);
                //System.out.println("Merging shared edges between ck and cn=" + cn.getSeqs() + " old dist=" + edge.getSeenDistances() + "+" + edgeJ.getSeenDistances() + " " + );
            }

            edges.put(ck, cn, new ThickEdge(ck, cn, dist, seenEdges));
        }

        for (Cluster cn : existingEdgesJ) {
            ThickEdge edge = edges.remove(cj, cn);
            edges.put(ck, cn, new ThickEdge(ck, cn, edge.getSeenDistances(), edge.getSeenEdges()));
        }

        return ck;
    }

    public void finish() throws IOException {
        mergesStream.close();
    }
}