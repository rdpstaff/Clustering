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
import java.util.List;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author fishjord
 */
@XmlRootElement
public class MergeDistsJob {

    private List<File> distFiles;
    private File outFile;

    public MergeDistsJob() {
    }

    public MergeDistsJob(List<File> distFiles, File outFile) {
        this.distFiles = distFiles;
        this.outFile = outFile;
    }

    public List<File> getDistFiles() {
        return distFiles;
    }

    public void setDistFiles(List<File> distFiles) {
        this.distFiles = distFiles;
    }

    public File getOutFile() {
        return outFile;
    }

    public void setOutFile(File outFile) {
        this.outFile = outFile;
    }

    private void merge(File f1, File f2, File outFile) throws IOException {
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

    public void run() throws Exception {
        if (distFiles.isEmpty()) {
            throw new IllegalArgumentException("Expected one or more distance files");
        }

        if (outFile.exists()) {
            if (!outFile.delete()) {
                throw new IOException(outFile.getAbsolutePath() + " already exists, attempt to delete failed, aborting");
            }
        }

        List<File> mergeFiles = new ArrayList(distFiles);

        while (mergeFiles.size() > 1) {
            List<File> nextMerge = new ArrayList();

            while (mergeFiles.size() > 1) {
                final File tmp = File.createTempFile("merge_dist", "", new File("."));
                nextMerge.add(tmp);

                final File f1 = mergeFiles.remove(0);
                final File f2 = mergeFiles.remove(0);

                try {
                    merge(f1, f2, tmp);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                f1.delete();
                f2.delete();
            }

            nextMerge.addAll(mergeFiles);

            mergeFiles = nextMerge;
        }

        FileUtils.moveFile(mergeFiles.get(0), outFile);
    }

    public static void main(String [] args) throws Exception {
        if(args.length < 3) {
            System.out.println("USAGE: MergeDistJob <output_file> <partition_file>...");
            return;
        }

        File outFile = new File(args[0]);
        List<File> inFiles = new ArrayList();
        for(int index = 1;index < args.length;index++) {
            inFiles.add(new File(args[index]));
        }

        new MergeDistsJob(inFiles, outFile).run();
    }
}
