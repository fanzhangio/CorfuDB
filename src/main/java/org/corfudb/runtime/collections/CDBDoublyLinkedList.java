package org.corfudb.runtime.collections;

import org.corfudb.runtime.AbstractRuntime;
import org.corfudb.runtime.DirectoryService;
import org.corfudb.runtime.StreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.function.UnaryOperator;

/**
 *
 */
public class CDBDoublyLinkedList<E> extends CDBAbstractList<E> {

    static Logger dbglog = LoggerFactory.getLogger(CDBDoublyLinkedList.class);
    static protected final HashMap<Long, CDBDoublyLinkedList> s_lists = new HashMap<>();

    static public CDBDoublyLinkedList findList(long loid) {
        synchronized (s_lists) {
            if (s_lists.containsKey(loid))
                return s_lists.get(loid);
            return null;
        }
    }

    public long m_head;
    public long m_tail;
    public HashMap<Long, CDBDoublyLinkedListNode<E>> m_nodes;
    public StreamFactory sf;
    public long oid;

    public void applyToObject(Object bs) {

        dbglog.debug("CDBNode received upcall");
        NodeOp<E> cc = (NodeOp<E>) bs;
        switch (cc.cmd()) {
            case NodeOp.CMD_READ_HEAD: applyReadHead(cc); break;
            case NodeOp.CMD_READ_TAIL: applyReadTail(cc); break;
            case NodeOp.CMD_WRITE_HEAD: applyWriteHead(cc); break;
            case NodeOp.CMD_WRITE_TAIL: applyWriteTail(cc); break;
        }
    }

    protected long applyReadHead(NodeOp<E> cc) {
        rlock();
        try {
            cc.setReturnValue(m_head);
        } finally {
            runlock();
        }
        return (long) cc.getReturnValue();
    }

    protected long applyReadTail(NodeOp<E> cc) {
        rlock();
        try {
            cc.setReturnValue(m_tail);
        } finally {
            runlock();
        }
        return (long) cc.getReturnValue();
    }

    protected void applyWriteHead(NodeOp<E> cc) {
        wlock();
        try {
            m_head = cc.oidparam();
        } finally {
            wunlock();
        }
    }

    protected void applyWriteTail(NodeOp<E> cc) {
        wlock();
        try {
            m_tail = cc.oidparam();
        } finally {
            wunlock();
        }
    }

    public CDBDoublyLinkedList(AbstractRuntime tTR, StreamFactory tsf, long toid) {
        super(tTR, tsf, toid);
        m_head = oidnull;
        m_tail = oidnull;
        sf = tsf;
        m_nodes = new HashMap<>();
        synchronized (s_lists) {
            assert(!s_lists.containsKey(oid));
            s_lists.put(oid, this);
        }
    }

    protected CDBDoublyLinkedListNode<E> nodeById_nolock(long noid) {
        assert(lockheld());
        return m_nodes.getOrDefault(noid, null);
    }

    protected CDBDoublyLinkedListNode<E> nodeById(long noid) {
        rlock();
        try {
            return m_nodes.getOrDefault(noid, null);
        } finally {
            runlock();
        }
    }

    protected long readhead() {
        NodeOp<E> cmd = new NodeOp<>(NodeOp.CMD_READ_HEAD, oid, oid);
        if (TR.query_helper(this, null, cmd))
            return (long) cmd.getReturnValue();
        rlock();
        try {
            return m_head;
        } finally {
            runlock();
        }
    }

    protected long readtail() {
        NodeOp<E> cmd = new NodeOp<>(NodeOp.CMD_READ_TAIL, oid, oid);
        if(TR.query_helper(this, null, cmd))
            return (long) cmd.getReturnValue();
        rlock();
        try {
            return m_tail;
        } finally {
            runlock();
        }
    }

    protected long readnext(CDBDoublyLinkedListNode<E> node) {

        NodeOp<E> cmd = new NodeOp<>(NodeOp.CMD_READ_NEXT, node.oid, node.oid);
        if (TR.query_helper(node, null, cmd))
            return (long) cmd.getReturnValue();
        node.rlock();
        try {
            return node.oidnext;
        } finally {
            node.runlock();
        }
    }

    protected long readprev(CDBDoublyLinkedListNode<E> node) {

        NodeOp<E> cmd = new NodeOp<>(NodeOp.CMD_READ_PREV, node.oid, node.oid);
        if (TR.query_helper(node, null, cmd))
            return (long) cmd.getReturnValue();
        node.rlock();
        try {
            return node.oidprev;
        } finally {
            node.runlock();
        }
    }

    protected E readvalue(CDBDoublyLinkedListNode<E> node) {

        NodeOp<E> cmd = new NodeOp<>(NodeOp.CMD_READ_VALUE, node.oid, node.oid);
        if (TR.query_helper(node, null, cmd))
            return (E) cmd.getReturnValue();
        node.rlock();
        try {
            return node.value;
        } finally {
            node.runlock();
        }
    }

    @Override
    public int size() {

        int size = 0;
        long nodeoid = readhead();
        while(nodeoid != oidnull) {
            size++;
            nodeoid = readnext(nodeById(nodeoid));
        }
        return size;
    }

    @Override
    public int indexOf(Object o) {

        E value;
        int index = 0;
        if(!isTypeE(o)) return -1;
        long oidnode = readhead();

        while(oidnode != oidnull) {
            CDBDoublyLinkedListNode<E> node = nodeById(oidnode);
            value = readvalue(node);
            if(value.equals(o))
                return index;
            oidnode = readnext(node);
            index++;
        }
        return -1;
    }

    @Override
    public int sizeview() {

        rlock();
        try {
            int size = 0;
            long oidnode = m_head;
            while (oidnode != oidnull) {
                size++;
                CDBDoublyLinkedListNode<E> node = nodeById_nolock(oidnode);
                assert(node != null);
                node.rlock();
                try {
                    oidnode = node.oidnext;
                } finally {
                    node.runlock();
                }
            }
            return size;
        } finally {
            runlock();
        }
    }

    @Override
    public int lastIndexOf(Object o) {

        if(!isTypeE(o)) return -1;
        int size = size();
        int index = size-1;

        E value;
        long oidnode = readtail();
        while(oidnode != oidnull) {
            CDBDoublyLinkedListNode<E> node = nodeById(oidnode);
            value = readvalue(node);
            if(value.equals(o))
                return index;
            oidnode = readprev(node);
            index--;
        }
        return -1;
    }

    @Override
    public boolean isEmpty() {
        long head = readhead();
        return head == oidnull;
    }

    @Override
    public boolean contains(Object o) {
        return indexOf(o) != -1;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if(!contains(o)) return false;
        }
        return true;
    }

    @Override
    public E get(int index) {

        int cindex=0;
        long nodeoid = readhead();
        while(nodeoid != oidnull) {
            CDBDoublyLinkedListNode<E> node = nodeById(nodeoid);
            if(index == cindex)
                return readvalue(node);
            nodeoid = readnext(node);
            cindex++;
        }
        return null;
    }

    @Override
    public E remove(int index) {

        int cindex=0;
        boolean found = false;
        long nodeoid = readhead();
        CDBDoublyLinkedListNode<E> node = null;

        while(nodeoid != oidnull) {
            node = nodeById(nodeoid);
            if(index == cindex) {
                found = true;
                break;
            }
            nodeoid = readnext(node);
            cindex++;
        }

        if(!found)
            return null;

        E result = readvalue(node);
        long oidnext = readnext(node);
        long oidprev = readprev(node);

        if(oidnext == oidnull && oidprev == oidnull) {
            // remove singleton from list. equivalent:
            // head = null;
            // tail = null;
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_HEAD, oidnull));
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_TAIL, oidnull));
        }

        else if(oidnext != oidnull && oidprev != oidnull) {
            // remove in middle of list. equivalent:
            // prev.next = next;
            // next.prev = prev;
            TR.update_helper(nodeById(oidnext), new NodeOp(NodeOp.CMD_WRITE_PREV, oidprev));
            TR.update_helper(nodeById(oidprev), new NodeOp(NodeOp.CMD_WRITE_NEXT, oidnext));
        }

        else if(oidprev == oidnull) {
            // remove at head of list. equivalent:
            // next.prev = null;
            // head = next;
            TR.update_helper(nodeById(oidnext), new NodeOp(NodeOp.CMD_WRITE_PREV, oidnull));
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_HEAD, oidnext));
        }

        else {
            // remove at tail of list. equivalent:
            // prev.next = null;
            // tail = prev;
            TR.update_helper(nodeById(oidprev), new NodeOp(NodeOp.CMD_WRITE_NEXT, oidnull));
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_TAIL, oidprev));
        }

        return result;
    }

    @Override
    public boolean remove(Object o) {
        int idx = indexOf(o);
        if (idx==-1) return false;
        remove(idx);
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean res = true;
        for (Object o : c) {
            res &= remove(o);
        }
        return res;
    }

    @Override
    public void replaceAll(UnaryOperator<E> op) {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public void clear() {
        TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_HEAD, oidnull));
        TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_TAIL, oidnull));
    }

    @Override
    public E set(int index, E element) {

        NodeOp<E> cmd;
        int cindex=0;
        CDBDoublyLinkedListNode<E> node;
        long nodeoid = readhead();

        while(nodeoid != oidnull) {
            node = nodeById(nodeoid);
            if(index == cindex) {
                cmd = new NodeOp<>(NodeOp.CMD_WRITE_VALUE, node.oid, element);
                TR.update_helper(node, cmd);
                return element;
            }
            nodeoid = readnext(node);
            cindex++;
        }
        return null;
    }

    @Override
    public void sort(Comparator<? super E> c) {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public Spliterator<E> spliterator() {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public Object[] toArray() {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public <E> E[] toArray(E[] a) {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public ListIterator<E> listIterator() {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public Iterator<E> iterator() {
        throw new RuntimeException("unimplemented");
    }

    private CDBDoublyLinkedListNode<E> allocNode(E e, long oidtail) {
        CDBDoublyLinkedListNode<E> newnode = new CDBDoublyLinkedListNode<>(TR, sf, e, DirectoryService.getUniqueID(sf), this);
        wlock();
        try {
            m_nodes.put(newnode.oid, newnode);
            TR.update_helper(newnode, new NodeOp<>(NodeOp.CMD_WRITE_VALUE, newnode.oid, e));
            TR.update_helper(newnode, new NodeOp(NodeOp.CMD_WRITE_PREV, newnode.oid, oidtail));
            TR.update_helper(newnode, new NodeOp(NodeOp.CMD_WRITE_NEXT, newnode.oid, oidnull));
            return newnode;
        } finally {
            wunlock();
        }
    }

    @Override
    public boolean add(E e) {

        long oidtail = readtail();
        CDBDoublyLinkedListNode<E> newnode = allocNode(e, oidtail);

        if(oidtail == oidnull) {

            // add to an empty list. It suffices to update the
            // head and tail pointers to point to the new node.
            assert(m_head == oidnull);
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_HEAD, newnode.oid, newnode.oid));
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_TAIL, newnode.oid, newnode.oid));

        } else {

            // add to a non-empty list. must point old tail.next to
            // to the new node, and update the tail pointer of the list.
            CDBDoublyLinkedListNode<E> tail = nodeById(oidtail);
            TR.update_helper(tail, new NodeOp(NodeOp.CMD_WRITE_NEXT, tail.oid, newnode.oid));
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_TAIL, newnode.oid, newnode.oid));
        }
        return true;
    }

    @Override
    public void add(int index, E e) {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean res = true;
        for(E o : c){
            res &= add(o);
        }
        return res;
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        throw new RuntimeException("unimplemented");
    }

}

