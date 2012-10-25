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

package edu.msu.cme.pyro.cluster.upgma;

import edu.msu.cme.pyro.cluster.dist.ThinEdge;
import edu.msu.cme.pyro.cluster.io.EdgeReader;
import edu.msu.cme.pyro.cluster.io.EdgeWriter;
import edu.msu.cme.pyro.cluster.io.LocalEdgeReader;
import java.io.File;

/**
 *
 * @author fishjord
 */
public class UPGMAReaderTest {

    private static void printEdge(ThinEdge e) {
        System.out.println(e.getSeqi() + " x " + e.getSeqj() + " = " + e.getDist());
    }

    public static void main(String [] args) throws Exception{
        File f = new File("test.edges");
        EdgeWriter writer = new EdgeWriter(f);
        writer.writeEdge(new ThinEdge(1, 2, 0));
        writer.writeEdge(new ThinEdge(1, 3, 1));
        writer.writeEdge(new ThinEdge(1, 4, 2));
        writer.writeEdge(new ThinEdge(1, 5, 3));
        writer.writeEdge(new ThinEdge(2, 3, 4));
        writer.writeEdge(new ThinEdge(2, 4, 5));
        writer.writeEdge(new ThinEdge(2, 5, 6));
        writer.close();

        EdgeReader reader = new LocalEdgeReader(f);
        ThinEdge e;

        System.out.println("Local edge reader");
        while((e = reader.nextThinEdge()) != null)
            printEdge(e);
        reader.close();
        System.out.println("\n");

        System.out.println("Random access edge file");
        RandomAccessEdgeFile raEdgeFile = new RandomAccessEdgeFile(f);
        while((e = raEdgeFile.nextThinEdge()) != null)
            printEdge(e);
        raEdgeFile.close();
        System.out.println("\n");

        System.out.println("Overwriting edges, yo");
        raEdgeFile = new RandomAccessEdgeFile(f);
        raEdgeFile.nextThinEdge();
        raEdgeFile.overwriteEdge(new ThinEdge(8, 8, 8));
        raEdgeFile.nextThinEdge();
        raEdgeFile.nextThinEdge();
        raEdgeFile.overwriteEdge(new ThinEdge(8, 8, 9));
        raEdgeFile.close();
        System.out.println("Edges overwritten...cross your fingers\n\n");

        System.out.println("Local edge reader");
        reader = new LocalEdgeReader(f);
        while((e = reader.nextThinEdge()) != null)
            printEdge(e);
        reader.close();
        System.out.println("\n");

        System.out.println("Random access edge file");
        raEdgeFile = new RandomAccessEdgeFile(f);
        while((e = raEdgeFile.nextThinEdge()) != null)
            printEdge(e);
        raEdgeFile.close();
        System.out.println("\n");
    }
}
