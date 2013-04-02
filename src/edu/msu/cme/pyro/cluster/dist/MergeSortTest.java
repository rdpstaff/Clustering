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

package edu.msu.cme.pyro.cluster.dist;

import edu.msu.cme.pyro.cluster.io.LocalEdgeReader;
import edu.msu.cme.pyro.cluster.io.EdgeWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author fishjord
 */
public class MergeSortTest {

    private static void merge(File f1, File f2, File outFile) throws IOException {
        LocalEdgeReader reader1 = new LocalEdgeReader(f1);
        LocalEdgeReader reader2 = new LocalEdgeReader(f2);
        EdgeWriter out = new EdgeWriter(outFile);

        ThinEdge edge1 = reader1.nextThinEdge();
        ThinEdge edge2 = reader2.nextThinEdge();
        while (edge1 != null || edge2 != null) {
            if (edge1 == null) {
                out.writeEdge(edge2);
                edge2 = reader2.nextThinEdge();
            } else if (edge2 == null) {
                out.writeEdge(edge1);
                edge1 = reader1.nextThinEdge();
            } else if (edge1.compareTo(edge2) < 0) {
                out.writeEdge(edge1);
                edge1 = reader1.nextThinEdge();
            } else {
                out.writeEdge(edge2);
                edge2 = reader2.nextThinEdge();
            }
        }

        out.close();
        reader2.close();
        reader1.close();
    }

    public static void main(String [] args) throws Exception {

        long startTime = System.currentTimeMillis();

        List<File> mergeFiles = new ArrayList();


        File outFile = new File("test.matrix");
        if(outFile.exists())
            outFile.delete();

        while (mergeFiles.size() > 1) {
            List<File> nextMerge = new ArrayList();
            ExecutorService executor = Executors.newFixedThreadPool(1);

            while (mergeFiles.size() > 1) {
                final File tmp = File.createTempFile("merge_dist", "", new File("."));
                nextMerge.add(tmp);

                final File f1 = mergeFiles.remove(0);
                final File f2 = mergeFiles.remove(0);
                executor.execute(new Runnable() {

                    public void run() {

                        try {
                            merge(f1, f2, tmp);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }

                        f1.delete();
                        f2.delete();

                    }
                });
            }

            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.DAYS);

            nextMerge.addAll(mergeFiles);

            mergeFiles = nextMerge;
        }

        FileUtils.moveFile(mergeFiles.get(0), outFile);
        System.out.println("Merge complete in " + (System.currentTimeMillis() - startTime) + "ms");
    }
}
