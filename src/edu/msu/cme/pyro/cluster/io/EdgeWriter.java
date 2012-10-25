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

import edu.msu.cme.pyro.cluster.dist.ThinEdge;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

/**
 *
 * @author fishjord
 */
public class EdgeWriter {

    private DataOutputStream edgeWriter;

    public EdgeWriter(File f) throws IOException {
        //edgeWriter = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(f), 4096)), 4096));
        edgeWriter = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f)));
    }

    public void writeEdge(ThinEdge edge) throws IOException {
        if(edge.getSeqi() == edge.getSeqj()) {
            throw new IOException("I refuse to write an identity edge");
        }
        
        edgeWriter.writeInt(edge.getSeqi());
        edgeWriter.writeInt(edge.getSeqj());
        edgeWriter.writeInt(edge.getDist());
    }

    public void close() throws IOException {
        edgeWriter.close();
    }
}
