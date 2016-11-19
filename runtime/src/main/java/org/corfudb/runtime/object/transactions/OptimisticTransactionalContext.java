package org.corfudb.runtime.object.transactions;

import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by mwei on 4/4/16.
 */
@Slf4j
public class OptimisticTransactionalContext extends AbstractTransactionalContext {

    /**
     * The timestamp of the first read in the system.
     *
     * @return The timestamp of the first read object, which may be null.
     */
    @Getter(lazy = true)
    private final long firstReadTimestamp = fetchFirstTimestamp();

    public OptimisticTransactionalContext(CorfuRuntime runtime) {
        super(runtime);
    }

    /** Get the root context (the first context of a nested txn)
     * which must be an optimistic transactional context.
     * @return  The root context.
     */
    private OptimisticTransactionalContext getRootContext() {
        AbstractTransactionalContext atc = TransactionalContext.getRootContext();
        if (!(atc instanceof OptimisticTransactionalContext)) {
            throw new RuntimeException("Attempted to nest two different transactional context types");
        }
        return (OptimisticTransactionalContext)atc;
    }

    /**
     * Get the first timestamp for this transaction.
     *
     * @return The first timestamp to be used for this transaction.
     */
    public synchronized long fetchFirstTimestamp() {
        if (TransactionalContext.isInNestedTransaction()) {
            // If we're in a nested transaction, the first read timestamp
            // needs to come from the root.
            return getRootContext().getFirstReadTimestamp();
        } else {
            // Otherwise, fetch a read token from the sequencer the linearize
            // ourselves against.
            long token = runtime.getSequencerView().nextToken(Collections.emptySet(), 0).getToken();
            log.trace("Set first read timestamp for tx {} to {}", transactionID, token);
            return token;
        }
    }

    /** The write set for this transaction.*/
    @Getter
    private Map<UUID, List<UpcallWrapper>> writeSet = new ConcurrentHashMap<>();

    /** The read set for this transaction. */
    @Getter
    private Set<UUID> readSet = new HashSet<>();

    int getNumberParentUpdates(UUID streamID) {
        AbstractTransactionalContext p = this;
        int num = 0;
        while ((p = p.getParentContext()) != null) {
            num += p.getWriteSet().get(streamID) == null ?
                    0 : p.getWriteSet().get(streamID).size();
        }
        return num;
    }

    int getNumberTotalUpdates(UUID streamID) {
        return getNumberParentUpdates(streamID) +
                (getWriteSet().get(streamID) == null ?
                0 : getWriteSet().get(streamID).size());
    }

    /** Helper function to get a write set for a particular stream.
     *
     * @param id    The stream to get a write set for.
     * @return      The write set for that stream, as an ordered list.
     */
    private List<UpcallWrapper> getWriteSet(UUID id) {
        return writeSet.getOrDefault(id, new LinkedList<>());
    }

    /**
     * Roll back the optimistic updates we have made to a proxy,
     * restoring the state of the underlying object.
     * @param proxy             The proxy which we are rolling back.
     * @param <T>               The type of the proxy's underlying object.
     */
    @Override
    public <T> void rollbackUnsafe(ICorfuSMRProxyInternal<T> proxy) {
        // starting at the write pointer, roll back any
        // updates to the object we've applied
        try {
            // can we rollback all the updates? if not, abort and sync
            if (proxy.getUnderlyingObject().isOptimisticallyUndoable()) {
                proxy.getUnderlyingObject()
                        .removeOptimisticUndoRecords()
                        .forEach(entry ->
                            proxy.getUndoTargetMap().get(entry.getSMRMethod())
                                .doUndo(proxy.getUnderlyingObject().getObjectUnsafe(),
                                        entry.getUndoRecord(), entry.getSMRArguments()));
                // Lift our transactional context
                proxy.getUnderlyingObject().clearOptimisticVersionUnsafe();
                proxy.getUnderlyingObject().setTXContextUnsafe(null);
            }
            throw new UnsupportedOperationException("Couldn't undo...");
        } catch (Exception e) {
            // rolling back failed, so we'll resort to getting fresh state
            proxy.resetObjectUnsafe(proxy.getUnderlyingObject());
            proxy.getUnderlyingObject().setTXContextUnsafe(null);
            proxy.syncObjectUnsafe(proxy.getUnderlyingObject(),
                    proxy.getVersion());
        }
    }

    /**
     * Sync the state of the proxy to the latest updates in the write
     * set for a stream.
     * @param proxy             The proxy which we are playing forward.
     * @param <T>               The type of the proxy's underlying object.
     */
    @Override
    public <T> void syncUnsafe(ICorfuSMRProxyInternal<T> proxy) {
        // first, if some other thread owns this object
        // we'll try waiting, but if they take too long
        // we should steal it from them
        if (    proxy.getUnderlyingObject()
                .getTXContextUnsafe() != null &&
                !proxy.getUnderlyingObject()
                .isTXOwnedByThisThread())
        {
            // TODO: this is not going to be effective until
            // we release the lock. The other tx will not be
            // able to complete until we do.
            // There has to be at least one element present here
            // otherwise we wouldn't be owned by another thread.
            //try {
            //    otherTXStack.peek().completionFuture
            //            .get(100, TimeUnit.MILLISECONDS);
            //} catch (InterruptedException | ExecutionException |
            //        TimeoutException e) {
            //    // We don't care if the tx is aborted or canceled.
            //}

            // need to rollback but that means we need to have a list
            // of tx from the other thread!
            //proxy.getUnderlyingObject().getTXContextUnsafe()
            //        .forEach(x -> {
            //            x.rollbackUnsafe(proxy);
            //        });

            // Ran out of ways to roll back, so we sync
            // from beginning again.
            rollbackUnsafe(proxy);
            proxy.syncObjectUnsafe(proxy.getUnderlyingObject(),
                    getFirstReadTimestamp());
            proxy.getUnderlyingObject().setTXContextUnsafe
                    (TransactionalContext
                            .getTransactionStack());
        }
        // next, if the version is incorrect, we need to
        // sync.
        if (proxy.getVersion() != getFirstReadTimestamp()) {
            proxy.syncObjectUnsafe(proxy.getUnderlyingObject(),
                    getFirstReadTimestamp());
        }

        // if we're in a nested transaction, we need to make
        // sure to play back the transactions of all the parents
        if (TransactionalContext.isInNestedTransaction()) {
            Iterator<AbstractTransactionalContext> iterator =
                    TransactionalContext
                        .getTransactionStack().descendingIterator();
            for (AbstractTransactionalContext ctx = iterator.next();
                    iterator.hasNext(); iterator.next()) {
                if (ctx == this) {
                    // if ourselves, then break
                    break;
                }
                // Sync. This will recursively go up the stack,
                // unfortunately, until we add a flag to not sync parents.
                ctx.syncUnsafe(proxy);
            }
        }
        // finally, if we have buffered updates in the write set,
        // we need to apply them.

        if ((getNumberTotalUpdates(proxy.getStreamID())
                != proxy.getUnderlyingObject().getOptimisticVersionUnsafe())) {
            proxy.getUnderlyingObject()
                    .setTXContextUnsafe(TransactionalContext.getTransactionStack());
            IntStream.range(proxy.getUnderlyingObject().getOptimisticVersionUnsafe()
                    - getNumberParentUpdates(proxy.getStreamID()),
                    getWriteSet(proxy.getStreamID()).size())
                    .mapToObj(x -> getWriteSet(proxy.getStreamID()).get(x))
                    .forEach(wrapper -> {
                        SMREntry entry  = wrapper.getEntry();
                        // Find the upcall...
                        ICorfuSMRUpcallTarget<T> target =
                                proxy.getUpcallTargetMap().get(entry.getSMRMethod());
                        if (target == null) {
                            throw new
                                    RuntimeException("Unknown upcall " + entry.getSMRMethod());
                        }
                        // Can we generate an undo record?
                        IUndoRecordFunction<T> undoRecordTarget =
                                proxy.getUndoRecordTargetMap().get(entry.getSMRMethod());
                        if (undoRecordTarget != null) {
                            entry.setUndoRecord(undoRecordTarget
                                    .getUndoRecord(proxy.getUnderlyingObject()
                                                    .getObjectUnsafe(), entry.getSMRArguments()));
                            entry.setUndoable(true);
                            proxy.getUnderlyingObject()
                                    .addOptimisticUndoRecord(entry);
                        } else {
                            proxy.getUnderlyingObject()
                                    .setNotOptimisticallyUndoable();
                        }
                        try {
                             wrapper.setUpcallResult(target.upcall(proxy.getUnderlyingObject()
                                    .getObjectUnsafe(), entry.getSMRArguments()));
                            proxy.getUnderlyingObject()
                                    .optimisticVersionIncrementUnsafe();
                             wrapper.setHaveUpcallResult(true);
                        }
                        catch (Exception e) {
                            log.error("Error: Couldn't execute upcall due to {}", e);
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    /** Access the underlying state of the object.
     *
     * @param proxy             The proxy making the state request.
     * @param accessFunction    The access function to execute.
     * @param <R>               The return type of the access function.
     * @param <T>               The type of the proxy.
     * @return                  The result of the access.
     */
    @Override
    public <R, T> R access(ICorfuSMRProxyInternal<T> proxy, ICorfuSMRAccess<R, T> accessFunction) {
        // First, we add this access to the read set.
        readSet.add(proxy.getStreamID());

        // Next, we check if the write set has any
        // outstanding modifications.
        try {
            return proxy.getUnderlyingObject().optimisticallyReadAndRetry((v, o) -> {
                // to ensure snapshot isolation, we should only read from
                // the first read timestamp.
                        // Either not in a TX
                if ( (!proxy.getUnderlyingObject().isTransactionallyModifiedUnsafe() ||
                        // Or we are the TX
                        proxy.getUnderlyingObject().getModifyingContextUnsafe() == this) &&
                        v == getFirstReadTimestamp() &&
                        getNumberTotalUpdates(proxy.getStreamID()) ==
                                proxy.getUnderlyingObject().getOptimisticVersionUnsafe()
                        ) {
                    return accessFunction.access(o);
                }
                throw new ConcurrentModificationException();
            });
        } catch (ConcurrentModificationException cme) {
            // It turned out version was wrong, so we're going to have to do
            // some work.
        }

        // Now we're going to do some work to modify the object, so take the write
        // lock.
        return proxy.getUnderlyingObject().write((v, o) -> {
            syncUnsafe(proxy);
            // We might have ended up with a _different_ object
            return accessFunction.access(proxy.getUnderlyingObject()
                    .getObjectUnsafe());
        });
    }

    /** Obtain the result for an upcall. Since we are executing on a single thread,
     * The result of the upcall is just the last one stored.
     * @param proxy         The proxy making the request.
     * @param timestamp     The timestamp of the request.
     * @param <T>           The type of the proxy.
     * @return              The result of the upcall.
     */
    @Override
    public <T> Object getUpcallResult(ICorfuSMRProxyInternal<T> proxy, long timestamp) {
        // if we have a result, return it.
        UpcallWrapper wrapper = writeSet.get(proxy.getStreamID()).get((int) timestamp);
        if (wrapper != null && wrapper.isHaveUpcallResult()){
            return wrapper.getUpcallResult();
        }
        // Otherwise, we need to sync the object
        return proxy.getUnderlyingObject().write((v,o) -> {
            syncUnsafe(proxy);
            UpcallWrapper wrapper2 = writeSet.get(proxy.getStreamID()).get((int) timestamp);
            if (wrapper2 != null && wrapper2.isHaveUpcallResult()){
                return wrapper2.getUpcallResult();
            }
            // If we still don't have the upcall, this must be a bug.
            throw new RuntimeException("Tried to get upcall during a transaction but" +
            " we don't have it even after an optimistic sync");
        });
    }

    /** Logs an update. In the case of an optimistic transaction, this update
     * is logged to the write set for the transaction.
     * @param proxy         The proxy making the request.
     * @param updateEntry   The timestamp of the request.
     * @param <T>           The type of the proxy.
     * @return              The "address" that the update was written to.
     */
    @Override
    public <T> long logUpdate(ICorfuSMRProxyInternal<T> proxy, SMREntry updateEntry) {
        writeSet.putIfAbsent(proxy.getStreamID(), new LinkedList<>());
        writeSet.get(proxy.getStreamID()).add(new UpcallWrapper(updateEntry));
        return writeSet.get(proxy.getStreamID()).size() - 1;
    }

    /** Determine whether or not we can abort all the writes
     * in this transactions write set.
     * @param streamID  The stream to check
     * @return  True, if all writes can be aborted.
     */
    @Override
    public boolean canUndoTransaction(UUID streamID) {
        // This is safe, because the thread checking
        // if a transaction will be undone will hold
        // the write lock - for that object.
        return writeSet.get(streamID).stream()
                .allMatch(x -> x.getEntry().isUndoable());
    }

    /**
     * Commit a transaction into this transaction by merging the read/write sets.
     *
     * @param tc The transaction to merge.
     */
    @SuppressWarnings("unchecked")
    public void addTransaction(AbstractTransactionalContext tc) {
        // make sure the txn is syncd for all proxies
        readSet.addAll(tc.getReadSet());
        tc.getWriteSet().entrySet().forEach(e-> {
            writeSet.putIfAbsent(e.getKey(), new LinkedList<>());
            writeSet.get(e.getKey()).addAll(e.getValue());
        });
    }

    /** Commit the transaction. If it is the last transaction in the stack,
     * write it to the log, otherwise merge it into a nested transaction.
     *
     * @return The address of the committed transaction.
     * @throws TransactionAbortedException  If the transaction was aborted.
     */
    @Override
    public long commitTransaction() throws TransactionAbortedException {

        // If the transaction is nested, fold the transaction.
        if (TransactionalContext.isInNestedTransaction()) {
            getParentContext().addTransaction(this);
            return AbstractTransactionalContext.FOLDED_ADDRESS;
        }

        // Otherwise, commit by generating the set of affected streams
        // and having the sequencer conditionally issue a token.
        Set<UUID> affectedStreams = writeSet.keySet();

        // For now, we have to convert our write set into a map
        // that we can construct a new MultiObjectSMREntry from.
        ImmutableMap.Builder<UUID, MultiSMREntry> builder =
                ImmutableMap.builder();
        writeSet.entrySet()
                .forEach(x -> builder.put(x.getKey(),
                                          new MultiSMREntry(x.getValue().stream()
                                                            .map(UpcallWrapper::getEntry)
                                                            .collect(Collectors.toList()))));
        Map<UUID, MultiSMREntry> entryMap = builder.build();
        MultiObjectSMREntry entry = new MultiObjectSMREntry(entryMap);

        // Now we obtain a conditional address from the sequencer.
        // This step currently happens all at once, and we get an
        // address of -1L if it is rejected.
        long address = runtime.getStreamsView()
                .acquireAndWrite(affectedStreams, entry, t->true, t->true, getFirstReadTimestamp());
        if (address == -1L) {
            log.debug("Transaction aborted due to sequencer rejecting request");
            abortTransaction();
            throw new TransactionAbortedException();
        }

        super.commitTransaction();

        return address;
    }
}
