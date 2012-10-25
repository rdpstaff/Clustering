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
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author fishjord
 */
public class Heap<E> {

    private List<E> nodes = new ArrayList();
    private Comparator<E> comp;
    private Map<E, Integer> heapMap = new HashMap();
    public long timeSpentRebuilding = 0;

    public Heap(Comparator<E> comp) {
        this.comp = comp;
    }

    public boolean contains(E elem) {
        return nodes.contains(elem);
    }

    public void rebuild() {
        for(int index = parent(nodes.size());index >= 0;index--) {
            rebuild(index);
        }
    }

    private void rebuild(int index) {
        long startTime = System.currentTimeMillis();
        int left = left(index);
        int right = right(index);

        int largest = index;

        if(left < nodes.size() && comp.compare(nodes.get(largest), nodes.get(left)) > 0)
            largest = left;

        if(right < nodes.size() && comp.compare(nodes.get(largest), nodes.get(right)) > 0)
            largest = right;

        if(largest != index) {
            swap(index, largest);
            rebuild(largest);
        }

        timeSpentRebuilding += (System.currentTimeMillis() - startTime);
    }

    public void insert(E elem) {
        int insertAt = nodes.size();
        nodes.add(elem);

        shuffleUp(insertAt);
    }

    public E deleteTop() {
        E root = nodes.get(0);
        delete(0);

        return root;
    }

    public boolean delete(E elem) {
        Integer index = heapMap.get(elem);
        if(index == null) {
            return false;
        }

        return delete(index);
    }

    private boolean delete(int index) {
        E tmp = nodes.get(index);
        swap(index, nodes.size() - 1);
        heapMap.remove(nodes.remove(nodes.size() - 1));

        if(index == nodes.size())
            return true;
        
        changed(index);

        return true;
    }

    public boolean changed(E elem) {
        Integer index = heapMap.get(elem);
        if(index == null) {
            return false;
        }

        changed(index);
        return true;
    }

    private void changed(int index) {
        int parent = parent(index);

        if (parent != 0 && comp.compare(nodes.get(index), nodes.get(parent)) <= 0) {
            shuffleUp(index);
        } else {
            shuffleDown(index);
        }
    }
    
    private void swap(int i1, int i2) {
        Collections.swap(nodes, i1, i2);
        heapMap.put(nodes.get(i1), i1);
        heapMap.put(nodes.get(i2), i2);
    }

    private void shuffleDown(int node) {
        int currNode = left(node);
        while (currNode < nodes.size()) {
            if (currNode + 1 < nodes.size() && comp.compare(nodes.get(currNode + 1), nodes.get(currNode)) <= 0) {
                currNode++;
            }

            if (comp.compare(nodes.get(parent(currNode)), nodes.get(currNode)) <= 0) {
                heapMap.put(nodes.get(currNode), currNode);
                break;
            } else {
                swap(currNode, parent(currNode));
                currNode = left(currNode);
            }
        }
    }

    private void shuffleUp(int insertAt) {
        E elem = nodes.get(insertAt);

        while (insertAt != 0 && comp.compare(nodes.get(insertAt), nodes.get(parent(insertAt))) <= 0) {
            if(sibling(insertAt) < nodes.size() && comp.compare(nodes.get(sibling(insertAt)), nodes.get(insertAt)) < 0) {
                heapMap.put(elem, insertAt);
                elem = nodes.get(sibling(insertAt));
                insertAt = sibling(insertAt);
            }
            
            swap(insertAt, parent(insertAt));
            insertAt = parent(insertAt);            
        }

        nodes.set(insertAt, elem);
        heapMap.put(nodes.get(insertAt), insertAt);
    }

    public E top() {
        if (nodes.isEmpty()) {
            return null;
        }

        return nodes.get(0);
    }

    public E secondTop() {
        if (nodes.size() <= 1) {
            return null;
        }
        if (nodes.size() == 2) {
            return nodes.get(1);
        }

        if (comp.compare(nodes.get(1), nodes.get(2)) < 0) {
            return nodes.get(1);
        } else {
            return nodes.get(2);
        }
    }

    public boolean validate() {
        return validate(System.err);
    }

    public boolean checkConsistenency(PrintStream err) {

        boolean ret = true;
        for(E edge : heapMap.keySet()) {
            int index = heapMap.get(edge);
            if(edge != nodes.get(index)) {
                err.println("Expected to find " + edge + " at nodes[" + index + "] but I found " + nodes.get(index) + " which should have been at " + heapMap.get(nodes.get(index)) + " what I was looking for is at " + nodes.indexOf(edge));
                ThickEdge e1 = (ThickEdge)edge;
                ThickEdge e2 = (ThickEdge)nodes.get(index);
                err.println(e1.getCi().getId() + " <=> " + e1.getCj().getId() + " = " + e1.getSeenDistances() + ", " + e1.getSeenEdges() + ", " + e1.getBound(0));
                err.println(e2.getCi().getId() + " <=> " + e2.getCj().getId() + " = " + e2.getSeenDistances() + ", " + e2.getSeenEdges() + ", " + e2.getBound(0));
                ret = false;
            }
        }

        for(E e : nodes) {
            if(!heapMap.containsKey(e)) {
                err.println("Failed to find e from nodes in the heap map...");
                ThickEdge e1 = (ThickEdge)e;
                err.println(e1.getCi().getId() + " <=> " + e1.getCj().getId() + " = " + e1.getSeenDistances() + ", " + e1.getSeenEdges() + ", " + e1.getBound(0));
                ret = false;
            }
        }

        if(!ret) {
            err.println("Nodes.size() = " + nodes.size());
            err.println("heapMap.size() = " + heapMap.size());
        }

        return ret;

    }

    public boolean validate(PrintStream err) {
        for (int index = 0; left(index) < nodes.size(); index++) {
            if (comp.compare(nodes.get(index), nodes.get(left(index))) > 0) {
                err.println("nodes[" + index + "]=" + nodes.get(index) + " > nodes[" + left(index) + "]=" + nodes.get(left(index)));
                return false;
            }

            if (right(index) < nodes.size() && comp.compare(nodes.get(index), nodes.get(right(index))) > 0) {
                err.println("nodes[" + index + "]=" + nodes.get(index) + " > nodes[" + right(index) + "]=" + nodes.get(right(index)));
                return false;
            }
        }

        boolean ret = true;
        for(E edge : heapMap.keySet()) {
            int index = heapMap.get(edge);
            if(edge != nodes.get(index)) {
                err.println("Expected to find " + edge + " at nodes[" + index + "] but I found " + nodes.get(index) + " which should have been at " + heapMap.get(nodes.get(index)) + " what I was looking for is at " + nodes.indexOf(edge));
                ThickEdge e1 = (ThickEdge)edge;
                ThickEdge e2 = (ThickEdge)nodes.get(index);
                err.println(e1.getCi().getId() + " <=> " + e1.getCj().getId() + " = " + e1.getSeenDistances() + ", " + e1.getSeenEdges() + ", " + e1.getBound(0));
                err.println(e2.getCi().getId() + " <=> " + e2.getCj().getId() + " = " + e2.getSeenDistances() + ", " + e2.getSeenEdges() + ", " + e2.getBound(0));
                ret = false;
            }
        }

        for(E e : nodes) {
            if(!heapMap.containsKey(e)) {
                err.println("Failed to find e from nodes in the heap map...");
                ThickEdge e1 = (ThickEdge)e;
                err.println(e1.getCi().getId() + " <=> " + e1.getCj().getId() + " = " + e1.getSeenDistances() + ", " + e1.getSeenEdges() + ", " + e1.getBound(0));
                ret = false;
            }
        }

        if(!ret) {
            err.println("Nodes.size() = " + nodes.size());
            err.println("heapMap.size() = " + heapMap.size());
        }

        return ret;
    }

    public void print(PrintStream out) {
        this.print(out, 0, "");
    }

    private void print(PrintStream out, int node, String indent) {
        if (node >= nodes.size()) {
            return;
        }

        out.println(indent + "node[" + node + "]=" + nodes.get(node));
        print(out, left(node), indent + "  ");
        print(out, right(node), indent + "  ");
    }

    private static int parent(int node) {
        return (node - 1) / 2;
    }

    private static int left(int node) {
        return node * 2 + 1;
    }

    private static int right(int node) {
        return node * 2 + 2;
    }

    private static int sibling(int node) {
        if(node % 2 == 0)
            return node - 1;
        return node + 1;
    }
}
