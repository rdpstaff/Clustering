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

import edu.msu.cme.pyro.cluster.utils.Cluster;

/**
 *
 * @author fishjord
 */
public class ThickEdge {
    private long seenDistances;
    private long seenEdges;
    private Cluster ci;
    private Cluster cj;
    /*private int lowerBound;
    private int upperBound;*/

    /*public ThickEdge(Cluster ci, Cluster cj) {//, double psi) {
        this.ci = ci;
        this.cj = cj;
        //this.psi = psi;
    }*/

    public ThickEdge(Cluster ci, Cluster cj, long seenDist, long seenEdges) {//, double psi) {
        this.ci = ci;
        this.cj = cj;
        this.seenDistances = seenDist;
        this.seenEdges = seenEdges;
        //this.psi = psi;
        //recalcDist();
    }

    public void addEdge(int dist) {
        seenEdges++;
        seenDistances += dist;
        //recalcDist();
    }

    public void mergeEdge(ThickEdge edge) {
        seenDistances += edge.getSeenDistances();
        seenEdges += edge.getSeenEdges();
        //recalcDist();
    }

    public long getSeenDistances() {
        return seenDistances;
    }

    public long getSeenEdges() {
        return seenEdges;
    }

    public Cluster getCi() {
        return ci;
    }

    public Cluster getCj() {
        return cj;
    }

    public int getBound(int psiOrLambda) {
        return (int)(seenDistances + psiOrLambda * (ci.getNumberOfSeqs() * cj.getNumberOfSeqs() - seenEdges)) / (ci.getNumberOfSeqs() * cj.getNumberOfSeqs());
    }

    /*public String toString() {
        return getBound(0) + "";
    }*/

    /*public int getLowerBound() {
        return (int)(seenDistances + UPGMAState.getInstance().getLambda() * (ci.getNumberOfSeqs() * cj.getNumberOfSeqs() - seenEdges)) / (ci.getNumberOfSeqs() * cj.getNumberOfSeqs());
    }

    public int getUpperBound() {
        return (int)(seenDistances + UPGMAState.getInstance().getPsi() * (ci.getNumberOfSeqs() * cj.getNumberOfSeqs() - seenEdges)) / (ci.getNumberOfSeqs() * cj.getNumberOfSeqs());
    }*/

    /*public void updateLambda(double lambda) {
        this.lambda = lambda;
        recalcDist();
    }

    private void recalcDist() {
        upperBound = (seenDistances + psi * (ci.size() * cj.size() - seenEdges)) / (ci.size() * cj.size());
        lowerBound = (seenDistances + lambda * (ci.size() * cj.size() - seenEdges)) / (ci.size() * cj.size());
    }

    public double getLowerBound() {
        return lowerBound;
    }

    public double getUpperBound() {
        return upperBound;
    }*/
}
