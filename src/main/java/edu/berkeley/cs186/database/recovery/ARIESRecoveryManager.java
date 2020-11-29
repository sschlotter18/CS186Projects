package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        Long prevLSN  = transactionTable.get(transNum).lastLSN;
        CommitTransactionLogRecord commitRecord = new CommitTransactionLogRecord(transNum, prevLSN);

        Long newLSN = logManager.appendToLog(commitRecord);

        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMMITTING);
        transactionTable.get(transNum).lastLSN = newLSN;

        logManager.flushToLSN(newLSN);

        return newLSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5_part1): implement
        Long prevLSN  = transactionTable.get(transNum).lastLSN;
        AbortTransactionLogRecord abortRecord = new AbortTransactionLogRecord(transNum, prevLSN);

        Long newLSN = logManager.appendToLog(abortRecord);

        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.ABORTING);
        transactionTable.get(transNum).lastLSN = newLSN;

        return newLSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5_part1): implement
        Long returnLSN = 0L;
        if (transactionTable.get(transNum).transaction.getStatus() == Transaction.Status.ABORTING) {
            returnLSN = rollbackToLSN(transNum, 0);
        }
        Long prevLSN  = transactionTable.get(transNum).lastLSN;
        if (returnLSN != 0L) {
            prevLSN = returnLSN;
        }
        EndTransactionLogRecord endRecord = new EndTransactionLogRecord(transNum, prevLSN);

        Long newLSN = logManager.appendToLog(endRecord);

        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMPLETE);
        transactionTable.get(transNum).lastLSN = newLSN;

        for (Long page : transactionTable.get(transNum).touchedPages) {
            dirtyPageTable.putIfAbsent(page, prevLSN);
        }
        transactionTable.remove(transNum);

        return newLSN;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * The general idea is starting the LSN of the most recent record that hasn't
     * been undone:
     * - while the current LSN is greater than the LSN we're rolling back to
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Flush if necessary
     *       - Update the dirty page table if necessary in the following cases:
     *          - You undo an update page record (this is the same as applying
     *            the original update in reverse, which would dirty the page)
     *          - You undo alloc page page record (note that freed pages are no
     *            longer considered dirty)
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private Long rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        long undoLSN = lastRecordLSN;
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);

        while (lastRecordLSN > LSN) {
            LogRecord lRecord = logManager.fetchLogRecord(lastRecordLSN);
            if (lRecord.isUndoable()) {
                Pair<LogRecord, Boolean> undone = lRecord.undo(undoLSN);
                Long newCLR = logManager.appendToLog(undone.getFirst());
                if (undone.getSecond()) {
                    pageFlushHook(newCLR);
                }
                //the if statements might be wrong, totally confused
                if (lRecord.getType() == LogType.UNDO_ALLOC_PAGE)  {
                    /**if (lastRecordLSN == dirtyPageTable.get(transNum)) {
                     dirtyPageTable.remove(transNum);
                     }*/
                    dirtyPageTable.remove(transNum);
                } else if (lastRecord.getType() == LogType.UNDO_UPDATE_PAGE) {
                    dirtyPageTable.put(lastRecord.getPageNum().get(), undone.getFirst().getLSN());
                }

                undone.getFirst().redo(diskSpaceManager, bufferManager);

                undoLSN = undone.getFirst().getLSN();
            }
            lastRecordLSN = lRecord.getPrevLSN().get();

        }

        return undoLSN;

        // TODO(proj5_part1) implement the rollback logic described above
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     *
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        TransactionTableEntry t = transactionTable.get(transNum);

        Long newLSN;
        Long toReturn;
        Long newLSN1;
        Long newLSN2;

        if (before.length <= (BufferManager.EFFECTIVE_PAGE_SIZE/2)) {
            UpdatePageLogRecord update1 = new UpdatePageLogRecord(transNum, pageNum, t.lastLSN, pageOffset, before, after);
            newLSN1 = logManager.appendToLog(update1);
            t.lastLSN = newLSN1;
            t.touchedPages.add(pageNum);
            toReturn = newLSN1;
        } else {
            UpdatePageLogRecord update1 = new UpdatePageLogRecord(transNum, pageNum, t.lastLSN, pageOffset, before, new byte[0]);
            UpdatePageLogRecord update2 = new UpdatePageLogRecord(transNum, pageNum, t.lastLSN, pageOffset, new byte[0], after);
            newLSN1 = logManager.appendToLog(update1);
            newLSN2 = logManager.appendToLog(update2);
            t.lastLSN = newLSN2;
            t.touchedPages.add(pageNum);
            toReturn = newLSN2;
        }

        if(!dirtyPageTable.containsKey(pageNum)) {
            dirtyPageTable.put(pageNum, newLSN1);
        }



        // TODO(proj5_part1): implement
        return toReturn;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long LSN = transactionEntry.getSavepoint(name);

        // TODO(proj5_part1): implement
        LogRecord lstRec = this.logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastLSN = lstRec.getLSN();
        if (lastLSN <= LSN) {
            return;

        }
        while(true) {
            if (lstRec.isUndoable()) {
                Pair<LogRecord, Boolean> undoLstRec = lstRec.undo(lastLSN);
                LogRecord frstRec = undoLstRec.getFirst();
                lastLSN = this.logManager.appendToLog(frstRec);
                if (undoLstRec.getSecond()) {
                    this.logManager.flushToLSN(frstRec.getLSN());

                }
                frstRec.redo(this.diskSpaceManager, this.bufferManager);
                transactionEntry.lastLSN = lastLSN;
                if (lstRec.getPageNum().isPresent()) {
                    Long k = lstRec.getPageNum().get();
                    if (dirtyPageTable.containsKey(k) && lstRec.getLSN() > LSN) {
                        dirtyPageTable.remove(k);
                    }
                }
                lstRec = frstRec;

            }
            Optional<Long> undoNxt = lstRec.getUndoNextLSN();
            if (undoNxt.isPresent()) {
                if (undoNxt.get() <= LSN) {
                    return;
                }
                lstRec = this.logManager.fetchLogRecord(undoNxt.get());
            } else if (lstRec.getPrevLSN().isPresent()){
                if (lstRec.getPrevLSN().get() <= LSN) {
                    return;
                }
                lstRec = this.logManager.fetchLogRecord(lstRec.getPrevLSN().get());
            }

        }
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        // TODO(proj5_part1): generate end checkpoint record(s) for DPT and transaction table

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                            dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                            dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    logManager.appendToLog(endRecord);

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }

        for (Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()) {
            if (!EndCheckpointLogRecord.fitsInOneRecord(dpt.size() + 1, 0, 0, 0)) {
                logManager.appendToLog(new EndCheckpointLogRecord(dpt, txnTable, touchedPages));
                dpt.clear();
            }
            dpt.put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            if (!EndCheckpointLogRecord.fitsInOneRecord(dpt.size(), txnTable.size() + 1, 0, 0)) {
                logManager.appendToLog(new EndCheckpointLogRecord(dpt, txnTable, touchedPages));
                dpt.clear();
                txnTable.clear();
            }
            txnTable.put(entry.getKey(), new Pair<>(entry.getValue().transaction.getStatus(), entry.getValue().lastLSN));
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.cleanDPT();

        return () -> {
            this.restartUndo();
            this.checkpoint();
        };
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
     *
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     * - The status's in the transaction table should be updated if its possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> complete is a possible transition,
     *   but committing -> running is not.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;

        Iterator<LogRecord> currRecord = logManager.scanFrom(LSN);
        while(currRecord.hasNext()) {
            LogRecord current = currRecord.next();
            if (current.type == LogType.BEGIN_CHECKPOINT) {
                Long update = current.getMaxTransactionNum().get();
                updateTransactionCounter.accept(update);
            }

            boolean isPart = false;
            if (current.type == LogType.ALLOC_PART || current.type == LogType.UNDO_ALLOC_PART ||
                    current.type == LogType.UNDO_FREE_PART || current.type == LogType.FREE_PART) {
                isPart = true;
            }
            boolean isPage = false;
            if (current.type == LogType.ALLOC_PAGE || current.type == LogType.UNDO_ALLOC_PAGE ||
                    current.type == LogType.UPDATE_PAGE || current.type == LogType.UNDO_UPDATE_PAGE ||
                    current.type == LogType.FREE_PAGE || current.type == LogType.UNDO_FREE_PAGE) {
                isPage = true;
            }
            if (isPart || isPage) {
                transactionOp(current, isPart, isPage);
            }

            if (current.type == LogType.COMMIT_TRANSACTION || current.type == LogType.ABORT_TRANSACTION
                    || current.type == LogType.END_TRANSACTION) {
                transactionStatusChange(current);
            }

            if (current.type == LogType.END_CHECKPOINT) {
                transactionEndCheckpoint(current);
            }

        }

        //loop through values
        for (TransactionTableEntry t: transactionTable.values()) {
            Long txn = t.transaction.getTransNum();
            if (t.transaction.getStatus() == Transaction.Status.COMMITTING) {
                t.transaction.cleanup();
                t.lastLSN = logManager.appendToLog(new EndTransactionLogRecord(txn, t.lastLSN));
                t.transaction.setStatus(Transaction.Status.COMPLETE);
            } else if (t.transaction.getStatus() == Transaction.Status.RUNNING) {
                t.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                t.lastLSN = logManager.appendToLog(new AbortTransactionLogRecord(txn, t.lastLSN));
            }

            if (t.transaction.getStatus() == Transaction.Status.COMPLETE) {
                transactionTable.remove(txn);
            }
        }

    }

    private void transactionEndCheckpoint(LogRecord current) {
        for (Long page: current.getDirtyPageTable().keySet()) {
            dirtyPageTable.put(page, current.getDirtyPageTable().get(page));
        }
        for (Map.Entry<Long, Pair<Transaction.Status, Long>> p : current.getTransactionTable().entrySet()) {
            long transNum = p.getKey();
            long lsn = p.getValue().getSecond();

            Transaction t = newTransaction.apply(transNum);
            TransactionTableEntry tableEntry = new TransactionTableEntry(t);
            if (!transactionTable.containsKey(transNum)) {
                transactionTable.put(transNum,tableEntry);
            }
            if (t.getStatus() == Transaction.Status.COMPLETE) {
                this.transactionTable.remove(t.getTransNum());
            }
            if (t.getStatus() != Transaction.Status.COMMITTING && t.getStatus() != Transaction.Status.RECOVERY_ABORTING) {
                Transaction.Status status = p.getValue().getFirst();
                t.setStatus(status);
            }
            tableEntry.lastLSN = Math.max(tableEntry.lastLSN, lsn);

        }

        for(Long transNum: current.getTransactionTouchedPages().keySet()){
            if(transactionTable.get(transNum).transaction.getStatus() != Transaction.Status.COMPLETE){
                for(Long t: current.getTransactionTouchedPages().get(transNum)){
                    transactionTable.get(transNum).touchedPages.add(t);
                    this.acquireTransactionLock(transactionTable.get(transNum).transaction, getPageLockContext(t), LockType.X);
                }
            }
        }



    }

    private void transactionStatusChange(LogRecord current) {
        Long transNum = current.getTransNum().get();
        if (!transactionTable.containsKey(transNum)) {
            Transaction newT = newTransaction.apply(transNum);
            TransactionTableEntry newE = new TransactionTableEntry(newT);
            transactionTable.put(transNum, newE);
        }
        TransactionTableEntry entry = transactionTable.get(transNum);
        entry.lastLSN = current.getLSN();
        if (current.type == LogType.COMMIT_TRANSACTION) {
            entry.transaction.setStatus(Transaction.Status.COMMITTING);
        } else if (current.type == LogType.ABORT_TRANSACTION) {
            entry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
        } else if (current.type == LogType.END_TRANSACTION) {
            entry.transaction.cleanup();
            entry.transaction.setStatus(Transaction.Status.COMPLETE);
        }
    }


    private void transactionOp(LogRecord current, boolean isPartitionOp, boolean isPageOp) {
        if (current.getTransNum().equals(Optional.empty())) {
            return;
        }
        Long transNum = current.getTransNum().get();
        if (!transactionTable.containsKey(transNum)) {
            Transaction newT = newTransaction.apply(transNum);
            TransactionTableEntry newE = new TransactionTableEntry(newT);
            transactionTable.put(transNum, newE);
        }
        TransactionTableEntry entry = transactionTable.get(transNum);
        entry.lastLSN = current.getLSN();
        if (isPageOp) {
            entry.touchedPages.add(current.getPageNum().get());
            acquireTransactionLock(entry.transaction, getPageLockContext(current.getPageNum().get()), LockType.X);
            if (current.type == LogType.UPDATE_PAGE || current.type == LogType.UNDO_UPDATE_PAGE) {
                if (!dirtyPageTable.containsKey(current.getPageNum().get())) {
                    dirtyPageTable.put(current.getPageNum().get(), current.getLSN());
                }
            } else if (current.type == LogType.FREE_PAGE || current.type == LogType.UNDO_ALLOC_PAGE) {
                if(dirtyPageTable.containsKey(current.getPageNum().get())) {
                    dirtyPageTable.remove(current.getPageNum().get());
                }
            }
        }

    }




    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */
    void restartRedo() {
        // TODO(proj5_part2): implement
        Long smallestLSN = Long.MAX_VALUE;
        for (Map.Entry<Long, Long> t : dirtyPageTable.entrySet()) {
            if (t.getValue() < smallestLSN) {
                smallestLSN = t.getValue();

            }

        }
        Iterator<LogRecord> logs = logManager.scanFrom(smallestLSN);
        while (logs.hasNext()) {
            LogRecord log = logs.next();
            if (log.isRedoable()) {
                if (isItPage(log)) {
                    if (dirtyPageTable.containsKey(log.getPageNum().get()) && log.getLSN() >= dirtyPageTable.get(log.getPageNum().get())) {
                        Page pg = bufferManager.fetchPage(getPageLockContext(log.getPageNum().get()).parentContext(), log.getPageNum().get(), false);
                        if (pg.getPageLSN() < log.getLSN()) {
                            log.redo(diskSpaceManager, bufferManager);

                        }

                    }

                }
                else if (isItPrtn(log)) {
                    log.redo(diskSpaceManager, bufferManager);

                }

            }

        }

    }

    /**
     * This method performs the redo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        // TODO(proj5_part2): implement
        List<Long> num = new ArrayList<>();
        List<Transaction> txn = new ArrayList<>();
        for (TransactionTableEntry t : transactionTable.values()) {
            if (t.transaction.getStatus() == Transaction.Status.RECOVERY_ABORTING) {
                num.add(t.lastLSN);
                txn.add(t.transaction);
            }
        }
        List<Pair<Long, Transaction>> pairs = new ArrayList<>();
        for (int i=0; i<num.size(); i++) {
            pairs.add(new Pair<>(num.get(i), txn.get(i)));
        }
        PriorityQueue<Pair<Long, Transaction>> pq = new PriorityQueue<>(new PairFirstReverseComparator<>());
        pq.addAll(pairs);

        while(!pq.isEmpty()) {
            Pair<Long, Transaction> firtonQ = pq.poll();
            Long L = firtonQ.getFirst();
            Transaction transaction = firtonQ.getSecond();

            LogRecord r = logManager.fetchLogRecord(L);
            //see if undoable
            if (r.isUndoable()) {
                Pair<LogRecord, Boolean> undone = r.undo(transactionTable.get(firtonQ.getSecond().getTransNum()).lastLSN);
                LogRecord uLogRec = undone.getFirst();

                Long lsnCLRrecord = logManager.appendToLog(uLogRec);
                transactionTable.get(r.getTransNum().get()).lastLSN = lsnCLRrecord;
                if (undone.getSecond()) {
                    logManager.flushToLSN(uLogRec.getLSN());
                }
                if(!dirtyPageTable.containsKey(r.getPageNum().get())) {
                    dirtyPageTable.put(r.getPageNum().get(), lsnCLRrecord);
                }
                LogType uRecord = uLogRec.type;
                //update DPT
                if (uRecord == LogType.ALLOC_PAGE || uRecord == LogType.FREE_PAGE
                        || uRecord == LogType.UNDO_ALLOC_PAGE || uRecord == LogType.UNDO_FREE_PAGE) {
                    dirtyPageTable.remove(r.getPageNum().get());
                } else if (r.getType() == LogType.UNDO_UPDATE_PAGE) {
                    if (!dirtyPageTable.containsKey(r.getPageNum().get())) {
                        dirtyPageTable.put(r.getPageNum().get(), uLogRec.getLSN());
                    }
                } else if (r.getType() == LogType.UPDATE_PAGE) {
                    dirtyPageTable.put(r.getPageNum().get(), uLogRec.getLSN());
                }
                uLogRec.redo(diskSpaceManager, bufferManager);
            }

            long newLsn;
            if (r.getUndoNextLSN().isPresent()) {
                newLsn = r.getUndoNextLSN().get();
            } else {
                newLsn = r.getPrevLSN().get();
            }
            //end transaction if newLSN is zero
            if (newLsn != 0) {
                pq.add(new Pair<>(newLsn, transaction));
            } else {
                endT(transaction);
            }
        }
    }

    private void endT(Transaction transaction) {
        transaction.cleanup();
        transaction.setStatus(Transaction.Status.COMPLETE);
        logManager.appendToLog(new EndTransactionLogRecord(transaction.getTransNum(), transactionTable.get(transaction.getTransNum()).lastLSN));
        transactionTable.remove(transaction.getTransNum());
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager. THIS IS SLOW
     * and should only be used during recovery.
     */
    private void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////


    boolean isItPrtn (LogRecord lRec) {
        if (lRec.getPartNum().isPresent()) return true ;
        return false;
    }

    boolean isItPage (LogRecord lRec) {
        if (lRec.getPageNum().isPresent()) return true;
        return false;
    }

    /**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                        lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
