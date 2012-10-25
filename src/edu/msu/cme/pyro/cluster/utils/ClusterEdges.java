/*
 * Ribosomal Database Project II
 * Copyright 2009 Michigan State University Board of Trustees
 */
package edu.msu.cme.pyro.cluster.utils;

import edu.msu.cme.pyro.cluster.dist.ThickEdge;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author farrisry
 */
public class ClusterEdges {

    protected Map<Cluster, Map<Cluster, ThickEdge>> edges = new LinkedHashMap<Cluster, Map<Cluster, ThickEdge>>();

    public Set<Cluster> get(Cluster ci) {
        if (edges.get(ci) != null) {
            return new LinkedHashSet<Cluster>(edges.get(ci).keySet());
        }
        return new LinkedHashSet<Cluster>();
    }

    public int size() {
        Set<ThickEdge> edgesSet = new LinkedHashSet();

        for(Map<Cluster, ThickEdge> map : edges.values()) {
            edgesSet.addAll(map.values());
        }

        return edgesSet.size();
    }

    public void put(Cluster ci, Cluster cj, ThickEdge edge) {
        if (edges.get(ci) == null) {
            edges.put(ci, new LinkedHashMap<Cluster, ThickEdge>());
        }
        edges.get(ci).put(cj, edge);
        if (edges.get(cj) == null) {
            edges.put(cj, new LinkedHashMap<Cluster, ThickEdge>());
        }
        edges.get(cj).put(ci, edge);
    }

    public ThickEdge remove(Cluster ci, Cluster cj) {
        if (edges.get(ci) != null) {
            edges.get(ci).remove(cj);
        }
        if (edges.get(cj) != null) {
            return edges.get(cj).remove(ci);
        }
        return null;
    }

    public ThickEdge get(Cluster ci, Cluster cj) {
        if(ci == null || cj == null || edges.get(ci) == null)
            return null;

        return edges.get(ci).get(cj);
    }

    public Collection<ThickEdge> getEdges(Cluster c) {
        if(edges.get(c) == null)
            return null;
        
        return edges.get(c).values();
    }
}
