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

import edu.msu.cme.pyro.cluster.utils.Cluster;
import edu.msu.cme.pyro.cluster.utils.ClusterFactory;
import java.io.File;
import java.io.IOException;

/**
 *
 * @author fishjord
 */
public class UPGMAClusterFactory extends ClusterFactory {

    public UPGMAClusterFactory(int psi) {
        edges = new UPGMAClusterEdges(psi);        
    }

    public UPGMAClusterFactory(int psi, File mergesFile) throws IOException {
        super(mergesFile);
        edges = new UPGMAClusterEdges(psi);
    }

    @Override
    public Cluster mergeCluster(Cluster ci, Cluster cj, int mergeDist) {
        Cluster ret = super.mergeCluster(ci, cj, mergeDist);
        
        return ret;
    }

    public UPGMAClusterEdges getEdgeHolder() {
        return (UPGMAClusterEdges)edges;
    }
}
