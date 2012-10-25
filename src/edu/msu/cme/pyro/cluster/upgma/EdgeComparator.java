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

import edu.msu.cme.pyro.cluster.dist.ThickEdge;
import java.util.Comparator;

/**
 *
 * @author fishjord
 */
public class EdgeComparator implements Comparator<ThickEdge> {
    private int psiOrLambda;

    public EdgeComparator(int psiOrLambda) {
        this.psiOrLambda = psiOrLambda;
    }

    public void setPsiOrLambda(int psiOrLambda) {
        this.psiOrLambda = psiOrLambda;
    }

    public int getBound() {
        return psiOrLambda;
    }

    public int compare(ThickEdge o1, ThickEdge o2) {
        int v1 = o1.getBound(psiOrLambda);
        int v2 = o2.getBound(psiOrLambda);

        return v1 - v2;
    }


}
