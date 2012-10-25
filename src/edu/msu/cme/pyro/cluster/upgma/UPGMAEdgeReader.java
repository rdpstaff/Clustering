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

import edu.msu.cme.pyro.cluster.dist.DistanceCalculator;
import edu.msu.cme.pyro.cluster.utils.Cluster;
import edu.msu.cme.pyro.cluster.utils.ClusterFactory;
import edu.msu.cme.pyro.cluster.dist.ThickEdge;
import edu.msu.cme.pyro.cluster.dist.ThinEdge;
import edu.msu.cme.pyro.cluster.utils.ClusterUtils;
import edu.msu.cme.pyro.cluster.io.LocalEdgeReader;
import java.io.File;
import java.io.IOException;

/**
 *
 * @author fishjord
 */
public class UPGMAEdgeReader {

    private static final ThinEdge maxThinEdge = new ThinEdge(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);

    /*private LocalEdgeReader[] readers = new LocalEdgeReader[3];
    private File[] tmpEdgeFiles = new File[2];
    private int readFrom = 0;*/
//    private final long maxLoadedEdges;
    private final ClusterFactory clustFactory;
    private int psi = Integer.MIN_VALUE;
    private int unknownLambda = 0;
    private long timeSpentLookingahead = 0;
    private int lookaheads = 0;
    private RandomAccessEdgeFile edgeFile;
    private long edgeCount = 0;
    //Allow for memory usage during merging and don't want to use -ALL- mem, drives the GC crazy
    private static final float MAX_MEM_RATIO = .85f;
    public long timeSpendReadingDists = 0;
    //private RandomAccessEdgeFile tmpEdgeFile;
    //private ThinEdge nextIgnore = maxThinEdge;

    public UPGMAEdgeReader(int psi, File f, ClusterFactory clustFactory, int numSeqs) throws IOException {
        this(psi, f, clustFactory, numSeqs, new File(System.getProperty("java.io.tmpdir")));
    }

    public UPGMAEdgeReader(int psi, File f, ClusterFactory clustFactory, int numSeqs, File workDir) throws IOException {
        this.psi = psi;
        this.clustFactory = clustFactory;
        this.edgeFile = new RandomAccessEdgeFile(f);
    }

    public int getUnknownLambda() {
        return unknownLambda;
    }

    public static int getPsiFromFile(File f) throws IOException {
        int ret = 0;

        ThinEdge edge;
        LocalEdgeReader reader = new LocalEdgeReader(f);

        while((edge = reader.nextThinEdge()) != null) {
            if(edge.getDist() > ret)
                ret = edge.getDist();
        }

        return ret;
    }

    private boolean lookAhead() throws IOException {
        System.out.println("Begining look ahead");
        boolean ret = false;

        lookaheads++;
        long startTime = System.currentTimeMillis();

        edgeFile.mark();
        
        ThinEdge edge = null;
        Cluster ci = null;
        Cluster cj = null;
        ThickEdge tk = null;
        while((edge = edgeFile.nextThinEdge()) != null) {
            if(edge.equals(maxThinEdge)) continue;

            ci = clustFactory.getCluster(edge.getSeqi());
            cj = clustFactory.getCluster(edge.getSeqj());

            if(ci == null || cj == null || ci == cj) continue;

            tk = clustFactory.getThickEdge(ci, cj);
            if(tk != null) {
                ret = true;
                tk.addEdge(edge.getDist());
                edgeFile.overwriteEdge(maxThinEdge);
                //newTempFile.writeEdge(edge);
            }
        }

        UPGMAClusterEdges edges = ((UPGMAClusterFactory)clustFactory).getEdgeHolder();

        edgeFile.reset();
        System.out.println("Lookahead completed in " + (System.currentTimeMillis() - startTime));
        timeSpentLookingahead += (System.currentTimeMillis() - startTime);

        return ret;
    }

    public boolean loadMoreEdges() throws IOException, CannotLoadMoreEdgesException {

        System.gc();
        ThinEdge edge = null;
        int edgesLoaded = 0;
        int currEdgesLoaded = clustFactory.getEdgeCount();

        long startTime = System.currentTimeMillis();

        if(ClusterUtils.getMemRatio() > MAX_MEM_RATIO) {
            if(lookAhead())
                return true;
            throw new CannotLoadMoreEdgesException();
        }


        Cluster ci = null;
        Cluster cj = null;
        ThickEdge tk = null;
        while(ClusterUtils.getMemRatio() < MAX_MEM_RATIO) {
            //long startTime = System.nanoTime();
            do {
                edge = edgeFile.nextThinEdge();
            } while(edge != null && edge.equals(maxThinEdge));
            //long timeToRead = System.nanoTime() - startTime;

            if(edge == null) break;
            if(edge.getDist() > psi)
                throw new IllegalArgumentException("Edge " + edge.getSeqi() + " x " + edge.getSeqj() + " = " + edge.getDist() + " is more distant than supplied psi value " + psi);

            //startTime = System.nanoTime();
            ci = clustFactory.getCluster(edge.getSeqi());
            cj = clustFactory.getCluster(edge.getSeqj());

            if(ci == null)
                ci = clustFactory.createSingleton(edge.getSeqi());
            if(cj == null)
                cj = clustFactory.createSingleton(edge.getSeqj());

            if(ci == cj) continue;

            tk = clustFactory.getThickEdge(ci, cj);
            if(tk == null) {
                tk = clustFactory.createThickEdge(ci, cj, edge);
                edgesLoaded++;
            } else {
                tk.addEdge(edge.getDist());
            }

            //System.out.println("Read in edge " + edge.getSeqi() + "\t" + edge.getSeqj() + "\t" + edge.getDist() + " in " + timeToRead + "ns, processed in " + (System.nanoTime() - startTime) + "ns");
            //DistanceUtils.printEdge(edge, System.out);
        }

        System.gc();
        if(edge != null) {
            unknownLambda = edge.getDist();
            System.out.println("UPGMA Edge reading done in " + (System.currentTimeMillis() - startTime) + ", last edge read dist=" + edge.getDist() + " started with " + currEdgesLoaded + " edges and loaded " + edgesLoaded + " edges, memory in use ratio=" + ClusterUtils.getMemRatio());
        }

        edgeCount = currEdgesLoaded + edgesLoaded;

        timeSpendReadingDists += (System.currentTimeMillis() - startTime);
        
        return edgesLoaded != 0;
    }

    public long edgeCount() {
        return edgeCount;
    }

    /*private boolean lookAhead() throws IOException {
        boolean ret = false;

        lookaheads++;
        long startTime = System.currentTimeMillis();
        RandomAccessEdgeFile newTempFile = new RandomAccessEdgeFile(File.createTempFile("used_edges", ".dist", new File(".")), "rw");

        edgeFile.mark();
        
        ThinEdge edge = null;
        while((edge = edgeFile.nextThinEdge()) != null) {
            while(nextIgnore.compareTo(edge) < 0) {
                newTempFile.writeEdge(nextIgnore);
                nextIgnore = tmpEdgeFile.nextThinEdge();
                if(nextIgnore == null)
                    nextIgnore = maxThinEdge;
            }

            Cluster ci = clustFactory.getCluster(edge.getSeqi());
            Cluster cj = clustFactory.getCluster(edge.getSeqj());

            if(ci == null || cj == null || ci == cj) continue;

            ThickEdge tk = clustFactory.getThickEdge(ci, cj);
            if(tk != null) {
                ret = true;
                tk.addEdge(edge.getDist());
                newTempFile.writeEdge(edge);
            }
        }

        tmpEdgeFile.close();
        tmpEdgeFile.delete();
        tmpEdgeFile = newTempFile;
        edgeFile.reset();
        timeSpentLookingahead += (System.currentTimeMillis() - startTime);

        return ret;
    }

    public boolean loadMoreEdges() throws IOException, CannotLoadMoreEdgesException {

        ThinEdge edge = null;
        int edgesLoaded = 0;
        int currEdgesLoaded = clustFactory.getEdgeCount();

        if(maxLoadedEdges == currEdgesLoaded) {
            if(lookAhead())
                return true;
            throw new CannotLoadMoreEdgesException();
        }

        while(currEdgesLoaded + edgesLoaded < maxLoadedEdges) {
            //long startTime = System.nanoTime();
            do {
                edge = edgeFile.nextThinEdge();
            } while(edge != null && tmpEdgeFile != null && !edge.equals(nextIgnore));
            //long timeToRead = System.nanoTime() - startTime;

            nextIgnore = tmpEdgeFile.nextThinEdge();
            if(nextIgnore == null)
                nextIgnore = maxThinEdge;

            if(edge == null) break;
            if(edge.getDist() > psi)
                throw new IllegalArgumentException("Edge " + edge.getSeqi() + " " + edge.getSeqj() + " " + edge.getDist() + " is more distant than supplied psi value " + psi);

            //startTime = System.nanoTime();
            Cluster ci = clustFactory.getCluster(edge.getSeqi());
            Cluster cj = clustFactory.getCluster(edge.getSeqj());

            if(ci == null)
                ci = clustFactory.createSingleton(edge.getSeqi());
            if(cj == null)
                cj = clustFactory.createSingleton(edge.getSeqj());

            if(ci == cj) continue;

            ThickEdge tk = clustFactory.getThickEdge(ci, cj);
            if(tk == null) {
                tk = clustFactory.createThickEdge(ci, cj);
                edgesLoaded++;
            }
            tk.addEdge(edge.getDist());

            //System.out.println("Read in edge " + edge.getSeqi() + "\t" + edge.getSeqj() + "\t" + edge.getDist() + " in " + timeToRead + "ns, processed in " + (System.nanoTime() - startTime) + "ns");
            //DistanceUtils.printEdge(edge, System.out);
        }

        if(edge != null) {
            unknownLambda = edge.getDist();
        }

        return edgesLoaded != 0;
    }*/

    /*private boolean lookAhead() throws IOException {
        boolean ret = false;

        lookaheads++;
        long startTime = System.currentTimeMillis();
        EdgeWriter out;
        if(readFrom == 2 || readFrom == 0) {
            out = new EdgeWriter(tmpEdgeFiles[0]);
        } else {
            out = new EdgeWriter(tmpEdgeFiles[1]);
        }

        /*if(readFrom == 0)
            System.out.println("Draining edges from original matrix file");
        else if(readFrom == 1)
            System.out.println("Draining edges from " + tmpEdgeFiles[0].getAbsolutePath());
        else if(readFrom == 2)
            System.out.println("Draining edges from " + tmpEdgeFiles[1].getAbsolutePath());
        else
            System.out.println("..fuck");

        ThinEdge edge = null;
        while((edge = readers[readFrom].nextThinEdge()) != null) {
            Cluster ci = clustFactory.getCluster(edge.getSeqi());
            Cluster cj = clustFactory.getCluster(edge.getSeqj());

            if(ci == null || cj == null || ci == cj) continue;

            ThickEdge tk = clustFactory.getThickEdge(ci, cj);
            if(tk == null) {
                out.writeEdge(edge);
            } else {
                ret = true;
                tk.addEdge(edge.getDist());
            }
        }

        Set<ThickEdge> edges = new HashSet();
        for(Cluster c : clustFactory.getClusters()) {
            if(clustFactory.getThickEdges(c) != null)
                edges.addAll(clustFactory.getThickEdges(c));
        }

        out.close();
        if(readFrom == 2 || readFrom == 0) {
            readFrom = 1;
            if(readFrom == 2)
                readers[2].close();
            readers[1] = new LocalEdgeReader(tmpEdgeFiles[0]);
            //System.out.println("Switching to reading edges from " + tmpEdgeFiles[0].getAbsolutePath());
        } else {
            readFrom = 2;
            readers[1].close();
            readers[2] = new LocalEdgeReader(tmpEdgeFiles[1]);
            //System.out.println("Switching to reading edges from " + tmpEdgeFiles[1].getAbsolutePath());
        }

        timeSpentLookingahead += (System.currentTimeMillis() - startTime);

        return ret;
    }*/

    /*public boolean loadMoreEdges() throws IOException, CannotLoadMoreEdgesException {

        ThinEdge edge = null;
        int edgesLoaded = 0;
        int currEdgesLoaded = clustFactory.getEdgeCount();
        
        if(maxLoadedEdges == currEdgesLoaded) {
            if(lookAhead())
                return true;
            throw new CannotLoadMoreEdgesException();
        }


        /*if(readFrom == 0)
            System.out.println("Loading more edges from original matrix file");
        else if(readFrom == 1)
            System.out.println("Loading more edges from " + tmpEdgeFiles[0].getAbsolutePath());
        else if(readFrom == 2)
            System.out.println("Loading more edges from " + tmpEdgeFiles[1].getAbsolutePath());
        else
            System.out.println("..fuck");

        while(currEdgesLoaded + edgesLoaded < maxLoadedEdges) {
            //long startTime = System.nanoTime();
            edge = readers[readFrom].nextThinEdge();
            //long timeToRead = System.nanoTime() - startTime;

            if(edge == null) break;
            if(edge.getDist() > psi)
                throw new IllegalArgumentException("Edge " + edge.getSeqi() + " " + edge.getSeqj() + " " + edge.getDist() + " is more distant than supplied psi value " + psi);

            //startTime = System.nanoTime();
            Cluster ci = clustFactory.getCluster(edge.getSeqi());
            Cluster cj = clustFactory.getCluster(edge.getSeqj());

            if(ci == null)
                ci = clustFactory.createSingleton(edge.getSeqi());
            if(cj == null)
                cj = clustFactory.createSingleton(edge.getSeqj());
            
            if(ci == cj) continue;

            ThickEdge tk = clustFactory.getThickEdge(ci, cj);
            if(tk == null) {
                tk = clustFactory.createThickEdge(ci, cj);
                edgesLoaded++;
            }
            tk.addEdge(edge.getDist());

            //System.out.println("Read in edge " + edge.getSeqi() + "\t" + edge.getSeqj() + "\t" + edge.getDist() + " in " + timeToRead + "ns, processed in " + (System.nanoTime() - startTime) + "ns");
            //DistanceUtils.printEdge(edge, System.out);
        }

        if(edge != null) {
            unknownLambda = edge.getDist();
        }

        return edgesLoaded != 0;
    }*/

    public int getLookaheads() {
        return lookaheads;
    }

    public long getTimeSpentLookingahead() {
        return timeSpentLookingahead;
    }
    
}
