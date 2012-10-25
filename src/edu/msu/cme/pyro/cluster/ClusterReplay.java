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
package edu.msu.cme.pyro.cluster;

import edu.msu.cme.pyro.cluster.dist.DistanceCalculator;
import edu.msu.cme.pyro.cluster.io.ClusterFileOutput;
import edu.msu.cme.pyro.cluster.io.ClusterOutput;
import edu.msu.cme.pyro.cluster.utils.Cluster;
import edu.msu.cme.pyro.cluster.utils.ClusterFactory;
import edu.msu.cme.pyro.derep.IdMapping;
import edu.msu.cme.pyro.derep.SampleMapping;
import edu.msu.cme.rdp.taxatree.Taxon;
import edu.msu.cme.rdp.taxatree.TaxonHolder;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author fishjord
 */
public class ClusterReplay {

    public static void main(String[] args) throws Exception {
        if (args.length != 4 && args.length != 5) {
            System.err.println("USAGE: ClusterReplay <idmapping> <sample_mapping> <merges_file> <cluster_out> [step = .01]");
            return;
        }

        IdMapping<Integer> idMapping = IdMapping.fromFile(new File(args[0]));
        SampleMapping<String> sampleMapping = SampleMapping.fromFile(new File(args[1]));

        ClusterOutput clustOut = new ClusterFileOutput(idMapping, sampleMapping, new PrintStream(new File(args[3])));
        ClusterFactory factory = new ClusterFactory();
        DataInputStream mergeStream = new DataInputStream(new BufferedInputStream(new FileInputStream(args[2])));

        double step = .01f;
        if (args.length == 5) {
            step = Double.valueOf(args[4]);
        }

        int realStep = (int) Math.round(step * DistanceCalculator.MULTIPLIER);
        int cutoff = 0;
        Map<Integer, Cluster> clustMap = new HashMap();

        System.err.println("Using step size " + step + " (realStep=" + realStep + ")");
        int dist = 0;

        try {
            while (true) {
                if (mergeStream.readBoolean()) { // Singleton
                    int cid = mergeStream.readInt();
                    int intId = mergeStream.readInt();

                    clustMap.put(cid, factory.createSingleton(intId));
                } else {
                    int ci = mergeStream.readInt();
                    int cj = mergeStream.readInt();
                    int ck = mergeStream.readInt();

                    Cluster clustI = clustMap.remove(ci);
                    Cluster clustJ = clustMap.remove(cj);

                    dist = mergeStream.readInt();

                    if (dist > cutoff) {
                        clustOut.printClusters(factory, cutoff);
                        System.out.println(((double) dist / DistanceCalculator.MULTIPLIER) + ": " + clustMap.size() + " clusters");
                        while (dist > cutoff) {
                            cutoff += realStep;
                        }
                    }

                    clustMap.put(ck, factory.mergeCluster(clustI, clustJ, dist));
                }
            }
        } catch (EOFException e) {
        }
        while (dist > cutoff) {
            cutoff += realStep;
        }
        System.out.println(((double) dist / DistanceCalculator.MULTIPLIER) + ": " + clustMap.size() + " clusters");

        clustOut.printClusters(factory, cutoff);
        clustOut.close();

        mergeStream.close();
    }
}
