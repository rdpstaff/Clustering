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

/**
 *
 * @author fishjord
 */
public class UPGMAState {

    private static class UPGMAStateHolder {
        private static UPGMAState holder = new UPGMAState();
    }

    protected UPGMAState() {}

    public static UPGMAState getInstance() {
        return UPGMAStateHolder.holder;
    }

    private int psi;
    private int lambda;

    public int getPsi() {
        return psi;
    }

    public int getLambda() {
        return lambda;
    }

    public void setPsi(int psi) {
        this.psi = psi;
    }

    public void setLambda(int lambda) {
        this.lambda = lambda;
    }
}
