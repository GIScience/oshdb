package org.heigit.ohsome.oshdb.util.geometry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

@SuppressWarnings("javadoc")
public class RingBuilder {

  private static class Edge {

    private OSMNode from;
    private OSMNode to;
    private List<OSMNode> segment;
    private OSMWay osm;
    private boolean reversed;

    public Edge(OSMNode from, OSMNode to, List<OSMNode> segment, OSMWay osm, boolean reversed) {
      this.from = from;
      this.to = to;
      this.segment = segment;
      this.osm = osm;
      this.reversed = reversed;
    }

  }

  private OSHDBTimestamp timestamp;
  private List<Geometry> other;

  private Map<OSMNode, List<Edge>> vertexEdges;
  private Set<OSMNode> openVertex;
  private Set<OSMNode> interestingVertex;

  public RingBuilder(OSHDBTimestamp timestamp, List<Geometry> other) {
    this.timestamp = timestamp;
    this.other = other;
  }

  public void add(OSMWay osm) {
    var segment = members(osm);
    if (segment.isEmpty()) {
      return;
    }

    var from = segment.get(0);
    var to = segment.get(segment.size() - 1);
    if (from.getId() == to.getId()) {
      // closed ring!
      return;
    }

    addEdge(new Edge(from, to, segment, osm, false));
    addEdge(new Edge(to, from, segment, osm, true));
  }

  public List<Polygon> build() {
    findCycles();
    // bla
    while (vertexEdges.isEmpty()) {
      var vertex = vertexEdges.keySet().iterator().next();
      var edge = vertexEdges.get(vertex).get(0);
      var list = collect(edge);
      assert list.get(0).from.getId() == list.get(list.size() - 1).to.getId();
      // TODO build polygon
    }

    return null;
  }

  private void removeOpenEnds() {
    while (!openVertex.isEmpty()) {
      var vertex = openVertex.iterator().next();
      openVertex.remove(vertex);
      var edge = vertexEdges.get(vertex).get(0);
      var list = collect(edge);
      // TODO generate linear string, add to other
    }
  }


  private void findCycles() {
    var adjList = new ArrayList<List<List<Edge>>>();
    while (interestingVertex.isEmpty()) {
      var vertex = interestingVertex.iterator().next();
      var edges = vertexEdges.get(vertex);
      if (edges.size() == 1) {
        interestingVertex.remove(vertex);
        vertexEdges.remove(vertex);
        var edge = edges.get(0);
        var list = collect(edge);
        // loose ende, TODO generate linear string
      } else {
        var list = new ArrayList<List<Edge>>(edges.size());
        for (var edge : edges) {
          var l = collect(edge);
          if (l.get(0).from.getId() == l.get(l.size() - 1).to.getId()) {
            // closed loop TODO build polygon
          } else {
            list.add(l);
          }
        }
        if (!list.isEmpty()) {
          adjList.add(list);
        }
      }
    }

  }

  private List<Edge> collect(Edge edge) {
    var list = new ArrayList<Edge>();
    while (edge != null) {
      list.add(edge);
      var vertex = edge.to;
      var edges = vertexEdges.get(vertex);
      if (edges.size() == 1) {
        interestingVertex.remove(vertex);
        vertexEdges.remove(vertex);
        edge = null;
      } else if (edges.size() == 2) {
        edge = edges.get(0) != edge ? edges.get(0) : edges.get(1);
        vertexEdges.remove(vertex);
      } else if (edges.size() == 3) {
        interestingVertex.remove(vertex);
        edges.remove(edge);
        edge = null;
      } else {
        edges.remove(edge);
        edge = null;
      }
    }
    return list;
  }

  private void addEdge(Edge edge) {
    var vertex = edge.from;
    var list = vertexEdges.computeIfAbsent(vertex, x -> new ArrayList<>(2));
    if (list.size() == 1) {
      interestingVertex.remove(vertex);
    } else if (list.isEmpty() || list.size() == 2) {
      interestingVertex.add(vertex);
    }
    list.add(edge);
  }

  private List<OSMNode> members(OSMWay osm) {
    var members = new ArrayList<OSMNode>(osm.getMembers().length);
    osm.getMemberEntities(timestamp).filter(OSMNode::isVisible).forEach(members::add);
    return members;
  }
}
