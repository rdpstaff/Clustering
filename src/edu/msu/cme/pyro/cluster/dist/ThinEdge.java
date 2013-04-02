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

/**
 *
 * @author fishjord
 */
public class ThinEdge implements Comparable<ThinEdge> {
    private int seqi;
    private int seqj;
    private int dist;

    public ThinEdge(int seqi, int seqj, int dist) {
        this.seqi = seqi;
        this.seqj = seqj;
        this.dist = dist;
    }

    public int getSeqi() {
        return seqi;
    }

    public int getSeqj() {
        return seqj;
    }

    public int getDist() {
        return dist;
    }

    public int compareTo(ThinEdge o) {
        return dist - o.getDist();
    }

    @Override
    public boolean equals(Object o) {
        if(ThinEdge.class != o.getClass())
            return false;

        ThinEdge edge = (ThinEdge)o;

        return seqi == edge.seqi && seqj == edge.seqj && dist == edge.dist;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 83 * hash + this.seqi;
        hash = 83 * hash + this.seqj;
        hash = 83 * hash + this.dist;
        return hash;
    }
}
