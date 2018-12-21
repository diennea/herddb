/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */

package herddb.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Combined strucutre linked list / hash map.
 * <p>
 * If necessary elements can be removed in O1 with {@link #remove(Object)}
 * </p>
 *
 * @author diego.salvi
 *
 * @param <E>
 */
public class ListWithMap<E> {

    /**
     * Pointer to first node.
     * Invariant: (head == null && tail == null) ||
     *            (head.prev == null && head.item != null)
     */
    private Node<E> head;

    /**
     * Pointer to last node.
     * Invariant: (head == null && tail == null) ||
     *            (tail.next == null && tail.item != null)
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

        if (ref == null) {
            return null;
        }

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
