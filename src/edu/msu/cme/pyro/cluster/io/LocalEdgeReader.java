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
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 *
 * @author fishjord
 */
public class LocalEdgeReader implements EdgeReader {
    private DataInputStream edgeStream;

    public LocalEdgeReader(File f) throws IOException {
        //edgeStream = new DataInputStream(new BufferedInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(f), 4096)), 4096));
        edgeStream = new DataInputStream(new BufferedInputStream(new FileInputStream(f)));
    }

    public ThinEdge nextThinEdge() throws IOException {
        return nextThinEdge(new ThinEdge());
    }

    public ThinEdge nextThinEdge(ThinEdge edge) throws IOException {
        try {
            edge.setSeqi(edgeStream.readInt());
            edge.setSeqj(edgeStream.readInt());
            edge.setDist(edgeStream.readInt());

            if(edge.getSeqi() == edge.getSeqj()) {
                throw new IOException("Identity edges can't be present in the column matrix");
            }

            return edge;
        } catch(EOFException e) {
            return null;
        }
    }

    public void close() throws IOException {
        edgeStream.close();
    }
}
