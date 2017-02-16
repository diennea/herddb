package herddb.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Combined strucutre linked list / hash map.
 * <p>
 * If necessari elements can be removed in <tt>O1</tt> with {@link #remove(Object)}
 * </p>
 *
 * @author diego.salvi
 *
 * @param <E>
 */
public class ListWithMap<E> {

    /**
     * Pointer to first node.
     * Invariant: (first == null && last == null) ||
     *            (first.prev == null && first.item != null)
     *
     * O sono nulli o il precedente di first è nullo e il suo elemento non nullo
     */
    private Node<E> head;

    /**
     * Pointer to last node.
     * Invariant: (first == null && last == null) ||
     *            (last.next == null && last.item != null)
     */
    private Node<E> tail;

    private Map<E,Node<E>> space;

    public ListWithMap() {

        head = tail = null;

        space = new HashMap<>();
    }

    public void append( E e ){
        final Node<E> ref = tail;
        final Node<E> node = new Node<>(ref, e, null);
        tail = node;

        if (ref == null) {
            head = node;
        } else {
            ref.next = node;
        }

        space.put(e, node);
    }

    public int size() {
        return space.size();
    }

    public boolean isEmpty() {
        return space.isEmpty();
    }

    public E peek() {
        final Node<E> ref = head;
        return (ref == null) ? null : ref.item;
    }

    public E poll() {

        final Node<E> ref = head;

        if (ref == null) {
            return null;
        }

        final E result = ref.item;
        final Node<E> next = ref.next;

        ref.item = null;
        ref.prev = null;
        ref.next = null;

        head = next;

        if (next == null) {
            tail = null;
        } else {
            next.prev = null;
        }

        space.remove(result);

        return result;
    }

    public boolean contains(E e) {
        return space.containsKey(e);
    }

    public E remove(E e) {

        final Node<E> ref = space.remove(e);

        if (ref != null) {

            if (ref.prev == null) {
                /* E' la testa */
                head = ref.next;
            } else {
                ref.prev.next = ref.next;
            }

            if (ref.next == null) {
                /* E' la coda */
                tail = ref.prev;
            } else {
                ref.next.prev = ref.prev;
            }

            final E rem = ref.item;

            ref.item = null;
            ref.prev = null;
            ref.next = null;

            return rem;
        }

        return null;
    }

    public void clear() {

        Node<E> ref = head;
        while(ref != null) {
            Node<E> next = ref.next;

            ref.item = null;
            ref.prev = null;
            ref.next = null;

            ref = next;
        }

        space.clear();
    }

    private static class Node<E> {
        E item;
        Node<E> prev;
        Node<E> next;

        Node(Node<E> prev, E element, Node<E> next) {
            this.item = element;
            this.prev = prev;
            this.next = next;
        }
    }

    public List<E> toList() {


        final List<E> list = new ArrayList<>(space.size());

        Node<E> ref = head;
        while(ref != null) {
            list.add(ref.item);
            ref = ref.next;
        }

        return list;

    }
}
