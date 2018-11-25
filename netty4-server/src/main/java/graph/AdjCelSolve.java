package graph;

/**
 * Created by moyong on 2017/10/30.
 */
import com.google.common.graph.*;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 在电信网络中，“服务小区”为用户提供了打电话、上网等服务。为了保证相邻服务小区的参数不冲突，经常会计算某个“服务小区”的一维邻区，甚至二维邻区。
 比如：“服务小区”A邻接B，B邻接C，A不邻接C，那么A的一维邻区是B，二维邻区是C。

 解决这类问题，用graph建模显然是再合适不过的。如果不关心图中两个节点的距离，应该选择MutableGraph<String>
 */
public class AdjCelSolve {
    private MutableGraph<String> graph;

    public AdjCelSolve(MutableGraph<String> graph) {
        this.graph = graph;
    }

    public static void main(String[] args) {

        MutableGraph<String> graph = buildGraph();
        AdjCelSolve adjCelSolve = new AdjCelSolve(graph);

        System.out.println(adjCelSolve.adjacentNodes("A", 1));
        System.out.println(adjCelSolve.adjacentNodes("A", 2));
        System.out.println(adjCelSolve.adjacentNodes("A", 3));
        System.out.println(adjCelSolve.adjacentNodes("A", 4));

        /*
        output:
        [B, C, D]
        [B, D, E]
        [D, E]
        [E]
         */
    }

    private Set<String> adjacentNodes(String sourceNode, int dimension) {
        Set<String> adjacentNodes = new HashSet<>();
        adjacentNodes.add(sourceNode);
        for (int i = 0; i < dimension; i++) {
            Set<String> currentDimensionNodes = new HashSet<>(adjacentNodes);
            adjacentNodes.clear();
            for (String adjacentNode : currentDimensionNodes) {
                adjacentNodes.addAll(graph.nodes().stream().filter(
                        node -> graph.successors(adjacentNode).contains(node)).collect(Collectors.toSet()));
            }
        }

        return adjacentNodes;
    }

    private static MutableGraph<String> buildGraph() {
        MutableGraph<String> graph = GraphBuilder.directed()
                .nodeOrder(ElementOrder.<String>natural()).allowsSelfLoops(false).build();

        graph.putEdge("A", "B");
        graph.putEdge("A", "C");
        graph.putEdge("A", "D");
        graph.putEdge("B", "D");
        graph.putEdge("C", "B");
        graph.putEdge("C", "E");
        graph.putEdge("D", "E");

        return graph;
    }
}
