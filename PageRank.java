import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.*;


public class PageRank {
    private int size;
    private int[] fromVertices;
    private int[] toVertices;
    //Map of vertices which are converged and inactive. All threads exit once all have converged
    final ConcurrentHashMap<Integer, Integer> convergedVertices = new ConcurrentHashMap<>();
    //Map of vertex and its value inorder to print in the end
    final ConcurrentHashMap<Integer, Double>  vertexValue = new ConcurrentHashMap<>();

    public PageRank(int size, int[] fromVertices, int[] toVertices) {
        this.size = size;
        this.fromVertices = fromVertices;
        this.toVertices = toVertices;
    }

    public void printOutput() {
        double sum = 0;
        for (int i = 0; i< size; i++) {
            System.out.println(vertexValue.get(i));
            sum = sum + vertexValue.get(i);
        }
        //System.out.println("sum " + sum);
        System.exit(0);
    }

    public void run() throws InterruptedException {

        //Map of vertex and its queue. Keeping two queues. One to read messages at superstep S and another to recieve messages for S+1
        ConcurrentHashMap<Integer, LinkedBlockingQueue<Double>> threadIdMessagesForStepS = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, LinkedBlockingQueue<Double>> threadIdMessagesForStepS1 = new ConcurrentHashMap<>();
        HashMap<Integer, LinkedList<Integer>> vertexNeighbours = new HashMap<>();

        //For synchronization
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(size);


        //Initialise the queues
        for(int i = 0; i <  size; i++) {
            threadIdMessagesForStepS.put(i, new LinkedBlockingQueue<>());
            threadIdMessagesForStepS1.put(i, new LinkedBlockingQueue<>());
        }

        //Create map of vertex and its neighours
        for (int k = 0; k < fromVertices.length; k++) {
            if(!vertexNeighbours.containsKey(fromVertices[k]))
                vertexNeighbours.put(fromVertices[k], new LinkedList());

            LinkedList<Integer> temp = vertexNeighbours.get(fromVertices[k]);
            temp.add(toVertices[k]);
            vertexNeighbours.put(fromVertices[k], temp);

        }
        //Connect sink vertices to all but itself
        for(int vertex = 0; vertex < size; vertex++) {
            if(!vertexNeighbours.containsKey(vertex)) {
                LinkedList<Integer> neighboursList = new LinkedList<>();
                for (int potentialNeighbour = 0; potentialNeighbour < size; potentialNeighbour++) {
                    if (potentialNeighbour != vertex)
                        neighboursList.add(potentialNeighbour);
                }
                vertexNeighbours.put(vertex, neighboursList);
            }
        }

        //Spawn threads for each vertex
        for(int j = 0; j <  size; j++) {
            Double val = (double) 1/size;
            vertexValue.put(j, val);
            Thread worker = new Thread(new PageRankVertex(j,  val, size, threadIdMessagesForStepS, threadIdMessagesForStepS1, true, vertexNeighbours.get(j), cyclicBarrier, convergedVertices, vertexValue));
            worker.setName("Thread " + j);
            worker.start();
        }

        //Wait until all threads exit
        while(convergedVertices.size() != size) {

        }
        printOutput();


    }

    public static void main(String[] args) {
        // Graph has vertices from 0 to `size-1`
        // and edges 1->0, 2->0, 3->0
        int size = 5;

        int[] from = {1,2,3, 4};
        int[] to = {0,0,0,0};


        PageRank pr = new PageRank(size, from, to);

        try {
            pr.run();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
