import java.text.NumberFormat;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;


public class PageRankVertex implements Vertex<Double, Double> {
    private static final Double damping = 0.85; // the damping ratio, as in the PageRank paper
    private static final Double tolerance = 1e-4; // the tolerance for converge checking


    private int vertexId;
    private Double value;
    private int numVertices;
    private ConcurrentHashMap<Integer, LinkedBlockingQueue<Double>> threadIdMessagesForStepS;
    private ConcurrentHashMap<Integer, LinkedBlockingQueue<Double>> threadIdMessagesForStepS1;
    private boolean isActiveFlag;
    private List<Integer> neighbours;
    protected CyclicBarrier cyclicBarrier;
    private ConcurrentHashMap<Integer, Integer> convergedVertices;
    private ConcurrentHashMap<Integer, Double>  vertexValue;


    public PageRankVertex(int vertexId, Double value, int numVertices, ConcurrentHashMap<Integer, LinkedBlockingQueue<Double>> threadIdMessagesForStepS, ConcurrentHashMap<Integer, LinkedBlockingQueue<Double>> threadIdMessagesForStepS1, boolean isActiveFlag,  List<Integer> neighbours, CyclicBarrier cyclicBarrier, ConcurrentHashMap<Integer, Integer> convergedVertices, ConcurrentHashMap<Integer, Double>  vertexValue) {
        this.vertexId = vertexId;
        this.value = value;
        this.numVertices = numVertices;
        this.threadIdMessagesForStepS = threadIdMessagesForStepS;
        this.threadIdMessagesForStepS1 = threadIdMessagesForStepS1;
        this.isActiveFlag = isActiveFlag;
        this.neighbours = neighbours;
        this.cyclicBarrier = cyclicBarrier;
        this.convergedVertices = convergedVertices;
        this.vertexValue = vertexValue;
    }

    @Override
    public int getVertexID() {
        return this.vertexId;
    }

    @Override
    public Double getValue() {
        return this.value;
    }

    @Override
    public void compute(Collection<Double> messages) {
        double sum = 0;
        for(Double message: messages) {
            sum += message.doubleValue();
        }
        value = 0.15/numVertices + damping*sum;
    }

    @Override
    public void sendMessageTo(int vertexID, Double message) {
        //initialize the queues for all vertices
        LinkedBlockingQueue<Double> existingList = threadIdMessagesForStepS1.get(vertexID);
        existingList.add(message);
        threadIdMessagesForStepS1.put(vertexID, existingList);
    }

    @Override
    public void voteToHalt() {
        isActiveFlag = false;
        convergedVertices.put(vertexId, 1);
    }

    @Override
    public void run() {
        int superstep = 0;

        //Stop the loop when all vertices converge
        while(convergedVertices.size() != numVertices) {

            //If there are no messages for this vertex then halt it
            if(threadIdMessagesForStepS1.get(vertexId).isEmpty() && isActiveFlag && superstep !=0) {
                voteToHalt();
            }

            //If there messages for this vertex but it's halted then remove it from converged map and make it active
            if(!threadIdMessagesForStepS1.get(vertexId).isEmpty()) {
                isActiveFlag = true;
                convergedVertices.remove(vertexId);
            }

            //Send messages even for superstep =0
            if (superstep == 0) {
                for (int neighbour : neighbours) {
                    sendMessageTo(neighbour, value/neighbours.size());
                }
            }

            //Call compute and send messages if vertex is active
            if(isActiveFlag && superstep > 0) {

                //Copy messages superstep S+1 to S
                threadIdMessagesForStepS.get(vertexId).clear();
                LinkedBlockingQueue<Double> temp = threadIdMessagesForStepS.get(vertexId);
                while(!threadIdMessagesForStepS1.get(vertexId).isEmpty()) {
                    Double message = threadIdMessagesForStepS1.get(vertexId).poll();
                    temp.add(message);
                }
                threadIdMessagesForStepS.put(vertexId, temp);
                threadIdMessagesForStepS1.get(vertexId).clear();


                //Call compute and also store previous value to check convergence
                Double previousValue = this.value;
                compute(threadIdMessagesForStepS.get(getVertexID()));
                Double currentValue = this.value;

                vertexValue.put(vertexId, currentValue);

                //Use cyclicBarrier before sending messages for synchronization
                try {
                    cyclicBarrier.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }

                //Send messages to all neighbours
                for (int neighbour : neighbours) {
                    sendMessageTo(neighbour, value/neighbours.size());
                }
                //Make the vertex inactove if the convergence condition is met
                if (Math.abs(previousValue - currentValue) < tolerance) {
                    voteToHalt();
                }
            }

            try {
                cyclicBarrier.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
            superstep = superstep +1;
        }

    }
}
