/*
 * Ribosomal Database Project II
 * Copyright 2009 Michigan State University Board of Trustees
 */
package edu.msu.cme.pyro.cluster.upgma;

import edu.msu.cme.pyro.cluster.dist.ThickEdge;
import edu.msu.cme.pyro.cluster.utils.Cluster;
import edu.msu.cme.pyro.cluster.utils.ClusterEdges;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author farrisry
 */
public class UPGMAClusterEdges extends ClusterEdges {

    public Heap<ThickEdge> ubHeap;
    public Heap<ThickEdge> lbHeap;
    private EdgeComparator lambda = new EdgeComparator(0);
    private boolean hyperValidating = false;

    public UPGMAClusterEdges(Integer psi) {
        ubHeap = new Heap<ThickEdge>(new EdgeComparator(psi));
        lbHeap = new Heap<ThickEdge>(lambda);
    }

    @Override
    public void put(Cluster ci, Cluster cj, ThickEdge tk) {
        super.put(ci, cj, tk);

        ubHeap.insert(tk);
        lbHeap.insert(tk);

        if(hyperValidating) {
            if (!ubHeap.validate()) {
		ubHeap.print(System.out);
                throw new IllegalStateException("Upperbound heap validation failed after inserting tk " + ci.getId() + " <=> " + cj.getId() + " = " + tk);
            }
            if (!lbHeap.validate()) {
                throw new IllegalStateException("Lowerbound heap validation failed after inserting tk " + ci.getId() + " <=> " + cj.getId() + " = " + tk);
            }
        }
    }

    @Override
    public ThickEdge remove(Cluster ci, Cluster cj) {
        ThickEdge tk = super.remove(ci, cj);
        deleteEdge(tk);
        return tk;
    }

    public void evict(ThickEdge tk) {
        if(super.remove(tk.getCi(), tk.getCj()) != tk) {
            throw new IllegalStateException("I've deleted an edge...but it wasn't the edge I expected?");
        }
        deleteEdge(tk);
    }

    private void deleteEdge(ThickEdge tk) {

        if (!ubHeap.delete(tk)) {
            throw new IllegalStateException("Failed to delete thick edge " + tk.getCi().getId() + " <=> " + tk.getCj().getId() + " = " + tk);
        }

        if (!lbHeap.delete(tk)) {
            throw new IllegalStateException("Failed to delete thick edge " + tk.getCi().getId() + " <=> " + tk.getCj().getId() + " = " + tk);
        }


        if(hyperValidating) {
            if (!ubHeap.validate()) {
                throw new IllegalStateException("Upperbound heap validation failed after deleting tk " + tk.getCi().getId() + " <=> " + tk.getCj().getId() + " = " + tk);
            }

            if (!lbHeap.validate()) {
                throw new IllegalStateException("Lowerbound heap validation failed after deleting tk " + tk.getCi().getId() + " <=> " + tk.getCj().getId() + " = " + tk);
            }
        }
    }

    public void setLambda(Integer lambda) {
        this.lambda.setPsiOrLambda(lambda);

        ubHeap.rebuild();
        lbHeap.rebuild();

        if (!ubHeap.validate()) {
            throw new IllegalStateException("Upper bound heap failed validation");
        }

        if (!lbHeap.validate()) {
            throw new IllegalStateException("Lower bound heap failed validation");
        }
    }

    public ThickEdge getLowestUB() {
        return ubHeap.top();
    }

    public ThickEdge getLowestLB() {
        return lbHeap.top();
    }

    public ThickEdge getSecondLowestLB() {
        return lbHeap.secondTop();
    }

    public Set<ThickEdge> getAllEdges() {
        Set<ThickEdge> ret = new HashSet();
        for(Map<Cluster, ThickEdge> edgeMap : edges.values()) {
            ret.addAll(edgeMap.values());
        }

        return ret;
    }
}
