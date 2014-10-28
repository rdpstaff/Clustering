/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msu.cme.pyro.cluster.io;

import edu.msu.cme.pyro.cluster.io.RDPClustParser.Cutoff;
import edu.msu.cme.pyro.cluster.utils.Cluster;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 *
 * @author Jordan Fish <fishjord at msu.edu>
 */
public class ClusterToBiom {

    private static void writeHeader(PrintStream out) {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        df.setTimeZone(tz);

        out.println("\"id\":null,");
        out.println("\"format\":\"Biological Observation Matrix 1.0.0\",");
        out.println("\"format_url\": \"http://biom-format.org\",");
        out.println("\"type\": \"OTU table\",");
        out.println("\"generated_by\": \"RDP mcClust\",");
        out.println("\"date\" : \"" + df.format(new Date()) + "\",");
    }

    private static void writeRowsHeader( Cutoff c, PrintStream out) {
        Map<String, List<Cluster>> clusters = c.getClusters();
        out.println("\"rows\" : [");
        HashSet<Integer> seenCluster = new HashSet<Integer>();
        int index = 0;
        for( List<Cluster> clusterList: clusters.values()) {
            for ( Cluster clust: clusterList){
                if ( !seenCluster.contains(clust.getId())) {
                    // should print out the cluster id
                    out.print("\t {\"id\" : \"cluster_" + clust.getId() +"\", \"metadata\" : null }");
                    if(index + 1 != c.getNumClusters()) {
                        out.print(",");
                    }
                    out.println();
                    seenCluster.add(clust.getId());
                    index++;
                }
            }
        }
        out.println("],");
    }

    private static void writeColsHeader(List<String> clusters, PrintStream out) {
        out.println("\"columns\" : [");
        for(int index = 0;index < clusters.size();index++) {
            out.print("\t {\"id\" : \"" + clusters.get(index) +"\", \"metadata\" : null }");
            if(index + 1 != clusters.size()) {
                out.print(",");
            }
            out.println();
        }
        out.println("],");
    }

    public static void writeCutoff(Cutoff c, PrintStream out) {
        Map<String, List<Cluster>> clusters = c.getClusters();
        List<String> sampleNames = new ArrayList(clusters.keySet());
        Collections.sort(sampleNames);
        out.println("{");

        writeHeader(out);
        writeRowsHeader(c, out);
        writeColsHeader(sampleNames, out);

        out.println("\"matrix_type\": \"dense\",");
        out.println("\"matrix_element_type\": \"int\",");
        out.println("\"shape\": [" + c.getNumClusters() + ", " + sampleNames.size() + "],");
        out.println("\"data\": [");
        for(int row = 0;row < c.getNumClusters();row++) {
            out.print("\t[");
            for(int col = 0;col < sampleNames.size();col++) {
                out.print(clusters.get(sampleNames.get(col)).get(row).getNumberOfSeqs());
                if(col + 1 != sampleNames.size()) {
                    out.print(",");
                }
            }
            out.print("]");
            if(row + 1 != c.getNumClusters()) {
                out.print(",");
            }
            out.println();
        }
        out.println("]");
        out.println("}");
    }

    public static void main(String[] args) throws IOException {
        if( args.length != 3){
            throw new IllegalArgumentException("Usage: clusterfile out.biom distance");
        }
        RDPClustParser parser = new RDPClustParser(new File(args[0]), false);

        PrintStream out = new PrintStream(args[1]);
        writeCutoff(parser.getCutoff(args[2]), out);
        out.close();
    }
}
