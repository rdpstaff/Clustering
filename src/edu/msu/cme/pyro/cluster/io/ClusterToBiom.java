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

    private static void writeRowsHeader(final int numClusters, PrintStream out) {
        out.println("\"rows\" : [");
        for(int index = 0;index < numClusters;index++) {
            out.print("\t {\"id\" : \"cluster_" + (index + 1) +"\", \"metadata\" : null }");
            if(index + 1 != numClusters) {
                out.print(",");
            }
            out.println();
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
        writeRowsHeader(c.getNumClusters(), out);
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
        RDPClustParser parser = new RDPClustParser(new File("test.clust"), false);

        PrintStream out = new PrintStream("test.biom");
        writeCutoff(parser.getCutoff("0.03"), out);
        out.close();
    }
}
