package graph;

/**
 * Created by moyong on 2017/10/30.
 */
import com.google.common.graph.ElementOrder;
import com.google.common.graph.MutableValueGraph;
import com.google.common.graph.ValueGraphBuilder;

import java.util.HashSet;
import java.util.Set;

/**
 * 单源最短路径问题，并且是带权有向无环图。因为对于无权图，用广度优先搜索算法就可以计算出最短路径了。
 * 对于本问题，我们采用经典的Dijkstra算法求解。
 */
public class DijkstraSolve {

    private final String sourceNode;
    private final MutableValueGraph<String, Integer> graph;

    public DijkstraSolve(String sourceNode, MutableValueGraph<String, Integer> graph) {
        this.sourceNode = sourceNode;
        this.graph = graph;
    }

    public static void main(String[] args) {
        MutableValueGraph<String, Integer> graph = buildGraph();
        //A 是起点
        DijkstraSolve dijkstraSolve = new DijkstraSolve("A", graph);

        dijkstraSolve.dijkstra();
        dijkstraSolve.printResult();
    }

    /**
     * 算法
     */
    private void dijkstra() {
        initPathFromSourceNode(sourceNode);

        Set<String> nodes = graph.nodes();
        if(!nodes.contains(sourceNode)) {
            throw new IllegalArgumentException(sourceNode +  " is not in this graph!");
        }

        Set<String> notVisitedNodes = new HashSet<>(graph.nodes());
        String currentVisitNode = sourceNode;
        while(!notVisitedNodes.isEmpty()) {
//            查找下一个节点
            String nextVisitNode = findNextNode(currentVisitNode, notVisitedNodes);
            if(nextVisitNode.equals("")) {
                break;
            }
            notVisitedNodes.remove(currentVisitNode);
            currentVisitNode = nextVisitNode;
        }
    }

    /**
     * 查询下一个节点
     *
     * @param currentVisitNode
     * @param notVisitedNodes
     * @return
     */
    private String findNextNode(String currentVisitNode, Set<String> notVisitedNodes) {
        int shortestPath = Integer.MAX_VALUE;
        String nextVisitNode = "";

        for (String node : graph.nodes()) {
            if(currentVisitNode.equals(node) || !notVisitedNodes.contains(node)) {
                continue;
            }

            if(graph.successors(currentVisitNode).contains(node)) {
                Integer edgeValue = graph.edgeValue(sourceNode, currentVisitNode).get() + graph.edgeValue(currentVisitNode, node).get();
                Integer currentPathValue = graph.edgeValue(sourceNode, node).get();
                if(edgeValue > 0) {
                    graph.putEdgeValue(sourceNode, node, Math.min(edgeValue, currentPathValue));
                }
            }

            if(graph.edgeValue(sourceNode, node).get() < shortestPath) {
                shortestPath = graph.edgeValue(sourceNode, node).get();
                nextVisitNode = node;
            }
        }

        return nextVisitNode;
    }

    /**
     * 初始化来自源节点的路径
     * @param sourceNode
     */
    private void initPathFromSourceNode(String sourceNode) {
        graph.nodes().stream().filter(
                node -> !graph.adjacentNodes(sourceNode).contains(node))
                .forEach(node -> graph.putEdgeValue(sourceNode, node, Integer.MAX_VALUE));
        graph.putEdgeValue(sourceNode, sourceNode, 0);
    }

    /**
     * 输出结果
     */
    private void printResult() {
        for (String node : graph.nodes()) {
            //源节点到目标节点的最短距离
            System.out.println(sourceNode + "->" + node + " shortest path is:" + graph.edgeValue(sourceNode, node));
        }
    }

    /**
     * 构造网络图
     * 有向图
     *
     * @return
     */
    private static MutableValueGraph<String, Integer> buildGraph() {
        MutableValueGraph<String, Integer> graph = ValueGraphBuilder.directed()
                .nodeOrder(ElementOrder.<String>natural()).allowsSelfLoops(true).build();

        graph.putEdgeValue("A", "B", 10);
        graph.putEdgeValue("A", "C", 3);
        graph.putEdgeValue("A", "D", 20);
        graph.putEdgeValue("B", "D", 5);
        graph.putEdgeValue("C", "B", 2);
        graph.putEdgeValue("C", "E", 15);
        graph.putEdgeValue("D", "E", 11);

        return graph;
    }
}
