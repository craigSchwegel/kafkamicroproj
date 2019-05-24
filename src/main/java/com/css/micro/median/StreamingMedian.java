/*
 * StreamingMedian is a stand alone class that takes an integer as input and calculates the
 * median for all the integers it has received.  The class is built to receive an unbounded
 * set of inputs such as a stream of integers and calculates the median on the fly without
 * having to sort.  The class uses a custom LinkedList data structure that inserts each
 * number in sorted order and keeps track of the median node.  The complexity of the class
 * is O(n) worst case run time as at most, it will insert the integer as the last element
 * in the list. The median calculation is a constant and is dropped from the big O representation.
 *
 * @author  Craig Schwegel
 * @version 1.0
 * @since   2019-05-24
 */
package com.css.micro.median;

public class StreamingMedian {

    class Node {
        int value;
        Node prev;
        Node next;
        int index;
        boolean bIsMedNode;
        public Node(int val)
        { value = val;}
    }

    Node medNode;
    Node root;
    Node tail;
    double median;
    int count;

    public static void main(String[] args)
    {
        try {
            StreamingMedian sm = new StreamingMedian();
            int[] intStreamArr = {2, 4, 19, 100000, 99, 0, 9, 8, 2, 3, 234, 1, 2, 5, 19};
            for (int i = 0; i < intStreamArr.length; i++) {
                sm.calcMedian(intStreamArr[i]);
                sm.printMedianAndLinkedList();
            }
        }catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }

    public double[] getStreamingMedianForArray(int[] input)
    {
        double[] medians = new double[input.length];
        StreamingMedian sm = new StreamingMedian();
        for (int i=0; i<input.length; i++)
            medians[i] = sm.calcMedian(input[i]);

        return medians;
    }

    public double calcMedian(int ival)
    {
        try {
            System.out.println("calcMedian(" + ival + ")");
            Node newNode = new Node(ival);
            //First number in stream
            if (count == 0) {
                root = newNode;
                tail = newNode;
                medNode = newNode;
                medNode.index = 1;
                medNode.bIsMedNode = true;
                median = ival;
                count++;
                return median;
            }
            //Second number in stream
            if (root.next == null) {
                if (root.value > ival) {
                    newNode.next = root;
                    tail = root;
                    root.prev = newNode;
                    root.bIsMedNode = false;
                    medNode = newNode;
                    medNode.bIsMedNode = true;
                    root = newNode;
                    median = ((double)root.value + (double)tail.value) / (double)2;
                    count++;
                } else {
                    newNode.prev = root;
                    tail = newNode;
                    root.next = newNode;
                    //medNode = root; //This isn't needed as it is already set to root
                    median = ((double)root.value + (double)tail.value) / (double)2;
                    count++;
                }
                return median;
            } //end if root == null

            //Handle n number of numbers from the stream
            Node curNode;

            boolean bInsertBeforeMedian = false;
            //start at root
            if (ival < median) {
                curNode = root;
                bInsertBeforeMedian = true;
            } else {
                curNode = medNode;
            }

            boolean bMoveForwardMedNode = false;

            while (true) {
                if (curNode.value < ival) {
                    if (curNode.next == null)
                    {
                        curNode.next = newNode;
                        newNode.prev = curNode;
                        tail = newNode;
                        count++;
                        break;
                    }
                } else {
                    if (curNode.prev != null)
                    {
                        Node previous = curNode.prev;
                        previous.next = newNode;
                        newNode.prev = previous;
                    }
                    else
                        root = newNode;
                    newNode.next = curNode;
                    curNode.prev = newNode;
                    count++;
                    break;
                }
                if (curNode.bIsMedNode && bInsertBeforeMedian)
                    bMoveForwardMedNode = true;
                curNode = curNode.next;
            } //end while (true)

            if (bMoveForwardMedNode)
            {
                medNode.bIsMedNode = false;
                medNode = medNode.next;
                medNode.bIsMedNode = true;
            }
            //Move median node
            int medIndex = medNode.index;
            double countMedian = (double) count / 2d;
            boolean bIsEven = (count % 2) == 0;
            //even number
            if (count > 2) {
                if (bInsertBeforeMedian && bIsEven) {
                    medNode.bIsMedNode = false;
                    medNode = medNode.prev;
                    medNode.bIsMedNode = true;
                    medNode.index--;
                    median = ((double) medNode.value + (double) medNode.next.value) / (double) 2;
                } else if (bInsertBeforeMedian && !bIsEven) {
                    //Median node doesn't move
                    median = medNode.value;
                } else if (!bInsertBeforeMedian && bIsEven) {
                    //Median node doesn't move
                    median = ((double) medNode.value + (double) medNode.next.value) / (double) 2;
                } else if (!bInsertBeforeMedian && !bIsEven) {
                    medNode.bIsMedNode = false;
                    medNode = medNode.next;
                    medNode.bIsMedNode = true;
                    medNode.index++;
                    median = medNode.value;
                }
            } //end if (count > 2)
            return median;
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return median;
        }
    }
    public void printMedianAndLinkedList()
    {
        try {
            if (root == null) {
                System.out.println("Linked List is empty.  Returning...");
                return;
            }

            Node n = root;
            StringBuilder sb = new StringBuilder();
            sb.append(n.value);
            while (true) {
                if (n.next != null)
                {
                    n = n.next;
                    sb.append("|").append(n.value);
                }
                else
                {
                    break;
                }
            }
            System.out.println("StreamingMedian.printMedianLinkedList():: " + sb.toString());
            System.out.println("StreamingMedian.printMedianLinkedList():: Median = " + median);
        } catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }
}
