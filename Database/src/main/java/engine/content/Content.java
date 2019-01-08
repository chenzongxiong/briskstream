package engine.content;

import engine.Meta.MetaTypes;
import engine.common.OrderLock;
import engine.storage.SchemaRecord;
import engine.storage.datatype.DataBox;
import engine.transaction.impl.TxnContext;

import java.util.List;

public interface Content {
    int CCOption_LOCK = 0;
    int CCOption_OrderLOCK = 1;
    int CCOption_LWM = 2;
    int CCOption_TStream = 3;
    int CCOption_SStore = 4;


    boolean TryReadLock();

    boolean TryWriteLock();

    void SetTimestamp(long timestamp);

    long GetTimestamp();

    void ReleaseReadLock();

    void ReleaseWriteLock();


    /**
     * new API for ordering guarantee
     */
    boolean TryWriteLock(OrderLock lock, TxnContext txn_context);

    boolean TryReadLock(OrderLock lock, TxnContext txn_context);

    boolean AcquireReadLock();

    boolean AcquireWriteLock();


    //TO.

    boolean RequestWriteAccess(long timestamp, List<DataBox> data);

    boolean RequestReadAccess(long timestamp, List<DataBox> data, boolean[] is_ready);

    void RequestCommit(long timestamp, boolean[] is_ready);

    void RequestAbort(long timestamp);

    //LWM

    long GetLWM();

//	LWMContentImpl.XLockQueue GetXLockQueue();

    SchemaRecord ReadAccess(TxnContext context, MetaTypes.AccessType accessType);

    SchemaRecord ReadAccess(long ts, MetaTypes.AccessType accessType);


    SchemaRecord readPreValues(long ts);

    SchemaRecord readValues(long ts);

    void updateValues(long ts, SchemaRecord value);

    boolean AcquireCertifyLock();

    void WriteAccess(long commit_timestamp, SchemaRecord local_record_);

    void ReleaseCertifyLock();

    void AddLWM(long ts);

    void DeleteLWM(long ts);

    boolean TryLockPartitions();

    void LockPartitions();

    void UnlockPartitions();
}
