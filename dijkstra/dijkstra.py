#!usr/bin/env python

import sys
import os
import time
from math import *
from nhpn import *
from priority_queue import *

def distance(node1, node2):
    """Returns the distance between node1 and node2, ignoring the Earth's 
    curvature.
    """
    latitude_diff = node1.latitude - node2.latitude
    longitude_diff = node1.longitude - node2.longitude
    return (latitude_diff**2 + longitude_diff**2)**.5

def distance_curved(node1, node2):
    """Returns the distance between node1 and node2, including the Earth's 
    curvature.
    """
    A = node1.latitude * pi / 10**6 / 180
    B = node1.longitude * pi / 10**6 / 180
    C = node2.latitude * pi / 10**6 / 180
    D = node2.longitude * pi / 10**6 / 180
    return acos(sin(A) * sin(C) + cos(A) * cos(C) * cos(B - D))

class NodeDistancePair(object):
    """Wraps a node and its distance representing it in the priority queue."""
    
    def __init__(self, node, distance):
        """Creates a NodeDistancePair to be used as a key in the priority queue.
        """
        self.node = node
        self.distance = distance
        
    def __lt__(self, other):
        # :nodoc: Delegate comparison to distance.
        return (self.distance < other.distance or 
                (self.distance == other.distance and 
                 id(self.node) < id(other.node)))
    
    def __le__(self, other):
        # :nodoc: Delegate comparison to distance.
        return (self.distance < other.distance or
                (self.distance == other.distance and 
                 id(self.node) <= id(other.node)))
                
    def __gt__(self, other):
        # :nodoc: Delegate comparison to distance.
        return (self.distance > other.distance or
                (self.distance == other.distance and 
                 id(self.node) > id(other.node)))
    
    def __ge__(self, other):
        # :nodoc: Delegate comparison to distance.
        return (self.distance > other.distance or
                (self.distance == other.distance and 
                 id(self.node) >= id(other.node)))
    
class Network(object):
    """The National Highway Planning network."""
    def __init__(self):
        """Creates a network with nodes, links and an edge set."""
        self.nodes, self.links = self._load_data()
        self._create_adjacency_lists()
        self.edge_set = self._create_edge_set()
    
    def __str__(self):
        """String representation of the network size."""
        num_nodes = len(self.nodes)
        num_edges = 0
        for node in self.nodes:
            num_edges += len(node.adj)
        return "Graph size: %d nodes, %d edges" % (num_nodes, num_edges)
        
    def verify_path(self, path, source, destination):
        """Verifies that path is a valid path from source to destination.
        
        Returns:
            True if the path is valid such that it uses only edges in the edge
            set.
            
        Raises:
            ValueError: if the first node and the last node do not match source
                and destination respectively or if the edge in not the the edge
                set.
        """
        if source != path[0]:
            raise ValueError('First node on a path is different form the \
                              source.')
        if destination != path[-1]:
            raise ValueError('Last node on a path is different form the \
                              destination.')
        for i in range(len(path) - 1):
            if (path[i], path[i+1]) not in self.edge_set and \
                (path[i+1], path[i]) not in self.edge_set:
                raise ValueError('Adjacent nodes in path have no edge between \
                                  them')
        return True

    def node_by_name(self, city, state):
        """Returns the first node that matches specified location.
        
        Args:
            city: the description of the node should include city.
            state: the state of the node should match state.
        
        Returns:
            The node if it exists, or None otherwise.
        """
    
        for node in self.nodes:
            if node.state == state:
                if city in node.description:
                    return node
        return None
    
    def _load_data(self):
        loader = Loader()
        lnodes = loader.nodes()
        llinks = loader.links()
        return lnodes, llinks
    
    def _create_adjacency_lists(self):
        # Use an adjacency list representation, by putting all vertices
        # adjacent to node in node.adj.
        for node in self.nodes:
            node.adj = []
        for link in self.links:
            link.begin.adj.append(link.end)
            link.end.adj.append(link.begin)
            
    def _create_edge_set(self):
        # Puts edges in a set for faster lookup.
        edge_set = set()
        for link in self.links:
            edge_set.add((link.begin, link.end))
        return edge_set
            

class PathFinder(object):
    """Finds a shortest path from the source to the destination in the network.
    """
    def __init__(self, network, source, destination):
        """Creates a PathFinder for the network with source and destination.
        
        Args:
            network: the network on which paths should be found.
            source: source of the path.
            destination: destination of the path.
        """    
        self.network = network
        self.source = source
        self.destination = destination
        
    def shortest_path(self, weight, algo):
        """Returns a PathResult for the shortest path from source to destination. 
        
        Args: 
            weight: weight function to compute edge weights.
            
        Returns:
            PathResult for the shortest path or None if path is empty.
        """
        start_time = time.clock()
        
        path, num_visited = algo(weight, self.network.nodes, 
                                          self.source, self.destination)
            
        time_used = round(time.clock() - start_time, 3)
        if path:
            if self.network.verify_path(path, self.source, self.destination):
                return PathResult(path, num_visited, weight, self.network, 
                                  time_used)
        else:
            return None


    def bidirectional_dijkstra(self, weight, nodes, source, destination):
        """Performs Bi-directional dijkstra's algorithm.
        It starts from both the ends and at each iteration it maintains
        two priority queues (one for storing keys in forward direction 
        of graph and one for storing backward direction of the graph)

        extract_min() for both forward and backward is performed alternatively
        at each step(order does not matter). A variable min_dist is used which
        is initiallized with None and its value is updated as soon as the the
        forward graph and backward graph is met and it is allowed to update
        if d_forward(u) + weight(u,w) + d_backward(w) < min_dist.

        The stopping condition is if the two extract_min() values sum to greater
        than or equal to min_dist. This is done because any more extract_min()
        will only increase the value of min_dist implying that min_dist can no
        longer be reduced, so it is the min distance

        At the end of the algorithm:
        - node.visited is True if the node is visited, False otherwise.
        (Node: node is visited if the shortest path to it is computed.)
        
        Args:
            weight: function for calculating the weight of the edge (u, v).
            nodes: list of all node in the network.
            source: the source node in the network.
            destination: the destination node in the network.

        Returns:
            A tuple: (the path as a list of nodes from source to destination,
                      the number of visited nodes)
        """
        
        pq_forward = PriorityQueue()
        pq_backward = PriorityQueue()
        
        dad_forward = {} 
        dad_backward = {} 
        for node in nodes:
            # I think visited flag is not required for nodes.
            # If implemented then it needs to have two visited flags
            # one for forward traversal and one for backward traversal
            node.forward_key_value = None
            node.backward_key_value = None

        dad_forward[source] = None
        dad_backward[destination] = None
        
        source.forward_key_value = NodeDistancePair(source, 0)
        destination.backward_key_value = NodeDistancePair(destination, 0)

        pq_forward.insert(source.forward_key_value)
        pq_backward.insert(destination.backward_key_value)

        # set min_dist to None
        min_dist = None

        path = [] 

        # this node is the joining node of the forward and backward paths
        meeting_point = None
        nodes_visited = 0
        while len(pq_forward) > 0 and len(pq_forward) > 0:
            f = pq_forward.extract_min()
            b = pq_backward.extract_min()
            f_node, f_dist = f.node, f.distance 
            b_node, b_dist = b.node, b.distance 
            nodes_visited += 2 
            # stopping criteria
            if min_dist is not None and (f_dist + b_dist >= min_dist):
                break

            # for the forward edges
            for child in f_node.adj:
                # check for d_forward(f_node) + weight(f_node, child) + d_backward(child)
                if child.backward_key_value is not None and b_dist > child.backward_key_value.distance: # this here can be optimized by using visited flag
                    # this is now optimized: the logic is if the current distance is greater than the child's
                    # distance, it means the child is already processed, as dijkstra progresses by increasing distance
                    if min_dist is None:
                        min_dist = f_dist + weight(f_node, child) + child.backward_key_value.distance
                        meeting_point = child
                        dad_forward[child] = f_node
                    elif min_dist > (f_dist + weight(f_node, child) + child.backward_key_value.distance):
                        min_dist = f_dist + weight(f_node, child) + child.backward_key_value.distance
                        meeting_point = child
                        dad_forward[child] = f_node # Note here

                if child.forward_key_value is None:
                    child.forward_key_value = NodeDistancePair(child, f_dist + weight(f_node, child))
                    pq_forward.insert(child.forward_key_value)
                    dad_forward[child] = f_node

                elif child.forward_key_value.distance > f_dist + weight(f_node, child):
                    child.forward_key_value.distance = f_dist + weight(f_node, child)
                    pq_forward.decrease_key(child.forward_key_value)
                    dad_forward[child] = f_node

            
            # for the backward edges
            for child in b_node.adj:
                if child.forward_key_value is not None and f_dist > child.forward_key_value.distance:
                    if min_dist is None:
                        min_dist = b_dist + weight(b_node, child) + child.forward_key_value.distance
                        meeting_point = b_node
                        dad_forward[b_node] = child
                    
                    elif min_dist > (b_dist + weight(b_node, child) + child.forward_key_value.distance):
                        min_dist = b_dist + weight(b_node, child) + child.forward_key_value.distance
                        meeting_point = b_node 
                        dad_forward[b_node] = child # the node closer to destination will be the meeting point in both cases 
                                                # and the meeting pt. will be a part of the forward graph

                if child.backward_key_value is None:
                    child.backward_key_value = NodeDistancePair(child, b_dist + weight(b_node, child))
                    pq_backward.insert(child.backward_key_value)
                    dad_backward[child] = b_node

                elif child.backward_key_value.distance > (b_dist + weight(b_node, child)):
                    child.backward_key_value.distance = b_dist + weight(b_node, child)
                    pq_backward.decrease_key(child.backward_key_value)
                    dad_backward[child] = b_node
        # insert the forward graph vertices in the path
        curr = meeting_point
        while curr is not None:
            path.append(curr)
            curr = dad_forward[curr]

        path.reverse()

        curr = dad_backward[meeting_point]
        while curr is not None:
            path.append(curr)
            curr = dad_backward[curr]

        return (path, nodes_visited)


    def dijkstra(self, weight, nodes, source, destination):
        """Performs Dijkstra's algorithm until it finds the shortest
        path from source to destination in the graph with nodes and edges.
        Assumes that all weights are non-negative.
    
        At the end of the algorithm:
        - node.visited is True if the node is visited, False otherwise.
        (Note: node is visited if the shortest path to it is computed.)
    
        Args:
            weight: function for calculating the weight of edge (u, v). 
            nodes: list of all nodes in the network.
            source: the source node in the network.
            destination: the destination node in the network.
         
        Returns:
            A tuple: (the path as a list of nodes from source to destination, 
                      the number of visited nodes)
        """
        Q = PriorityQueue()
        path = []	
        parent = {}
        parent[source] = None
        num_visited = 0
        for node in nodes:
            node.visited = False
            node.key_value = None
            
        source.key_value = NodeDistancePair(source, 0)
        Q.insert(source.key_value)
        while len(Q) > 0:
            curr_key = Q.extract_min()
            curr_node, curr_dist = curr_key.node, curr_key.distance	
            curr_node.visited = True
            num_visited += 1
            if curr_node is destination:
                break
            for child in curr_node.adj:
                if child.key_value is None:
                    child.distance = curr_dist + weight(curr_node, child)
                    child.key_value = NodeDistancePair(child, child.distance)
                    Q.insert(child.key_value)
                    parent[child] = curr_node
                elif child.key_value.distance > (curr_dist + weight(curr_node, child)):
                    child.key_value.distance = curr_dist + weight(curr_node, child)
                    parent[child] = curr_node
                    Q.decrease_key(child.key_value)
                        
        curr = destination
        while curr is not None:
            path.append(curr)
            curr = parent[curr]
        path.reverse()
        return (path, num_visited) 
            
    @staticmethod
    def from_file(file, network):
        """Creates a PathFinder object with source and destination read from 
        file.
        
        Args:
            file: file containing source and destination.
            network: network in which a shortest path needs to be found.
        
        Returns:
            A PathFinder object.
            
        Raises:
            ValueError: when source or destination is not valid.
        """
        source = destination = None
        for i in range(2):
            command = file.readline().split()
            city = ' '.join(command[1].split('_')).upper()
            node = network.node_by_name(city, command[2].upper())
            if command[0] == 'source':
                source = node
            elif command[0] == 'destination':
                destination = node
                
        if source and destination:
            return PathFinder(network, source, destination)
        else:
            if source is None:
                raise ValueError('Invalid source.')
            if destination is None:
                raise ValueError('Invalid destination.')  
    
class PathResult(object):
    """An object containing the results of a path found by PathFinder."""
    
    def __init__(self, path, num_visited, weight, network, time):
        """Creates a PathResult.
        
        Args:
            path: a list of nodes in the path.
            num_visited: number of nodes visited during path finding.
            weight: function to compute the weight of an edge (u, v).
            network: the network on which the path is found.
            time: time used to find the path.
        """
        self.network = network
        self.path = path
        self.num_visited = num_visited
        self.total_weight = self._total_weight(weight)
        self.time = time
        
    def to_kml(self):
        """Returns the path in kml format."""
        
        kml = ["""<?xml version="1.0" encoding="utf-8"?>
<kml xmlns="http://earth.google.com/kml/2.1">
  <Document>
    <Placemark>
      <LineString>
        <extrude>1</extrude>
        <tessellate>1</tessellate>
        <coordinates>
"""]
        kml.append(''.join("%f,%f\n" % (node.longitude/1000000., 
                                        node.latitude/1000000.) 
                           for node in self.path))
        kml.append("""</coordinates>
      </LineString>
    </Placemark>
  </Document>
</kml>
""")
        return ''.join(kml)
    
    def to_lines(self):
        """Returns a list of lines representing the results."""
        source = self.path[0]
        dest = self.path[-1]
        list = ["Path: %s, %s -> %s, %s" %  (source.description, \
                    source.state, dest.description, dest.state)]
        list.append(str(self.network))
        list.append("Nodes visited: %d" % self.num_visited)
        list.append("Path length: %.4f" % self.total_weight)
        list.append("Number of roads: %d" % (len(self.path) - 1))
        list.append("Time used in seconds: %.3f" % self.time)
        return list
    
    def sol_to_lines(self):
        return ["Path length: %.4f" % self.total_weight]
    
    def to_file(self, file):
        """Outputs to an output stream."""
        for line in self.to_lines():
            file.write(line)
            file.write("\n")
    
    def sol_to_file(self, file):
        """Outputs solution to output stream."""
        for line in self.sol_to_lines():
            file.write(line)
            file.write("\n")
                    
    def _total_weight(self, weight):
        """Computes the sum of weights along a path.
        
        Args:
            weight: function to compute the weight of an edge (u, v).
        """
        sum = 0
        for i in range(len(self.path) - 1):
            sum += weight(self.path[i], self.path[i + 1])
        return sum

# Command-line controller.
if __name__ == '__main__':
    network = Network()
    if os.environ.get('TRACE') == 'kml':
        pf = PathFinder.from_file(sys.stdin, network)
        with open('path_flat.kml', 'w') as file:
            r = pf.shortest_path(distance, pf.dijkstra)
            r and file.write(r.to_kml())
        with open('path_curved.kml', 'w') as file:
            r = pf.shortest_path(distance_curved, pf.dijkstra)
            r and file.write(r.to_kml())
    else:
        pf = PathFinder.from_file(sys.stdin, network)
        r = pf.shortest_path(distance, pf.bidirectional_dijkstra)
        if r:
            if os.environ.get('TRACE') == 'sol':
                r.sol_to_file(sys.stdout)
            else:
                r.to_file(sys.stdout)
        else:
            print 'No path is found.'

