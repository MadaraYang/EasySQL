package com.madara.tm;

import com.madara.server.tm.TransactionManager;
import com.madara.server.tm.impl.TransactionManagerImpl;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class TransactionsManagerTest {
    @Test
    public void createTransactionTest() {
        TransactionManager manager = TransactionManager.create("E:/MyDBTest/test_1");
        manager.begin();
        manager.begin();
        boolean active = manager.isActive(1);
        log.debug("trans1:{}",active);
        boolean abort = manager.isAbort(2);
        log.debug("trans2:{}",abort);
    }
    @Test
    public void openTransactionTest() {
        TransactionManager manager = TransactionManager.open("E:/MyDBTest/test_1");
        manager.begin();
        manager.begin();
        boolean active = manager.isActive(3);
        log.debug("trans3:{}",active);
        boolean abort = manager.isAbort(4);
        log.debug("trans4:{}",abort);
        manager.commit(1);
        log.debug("trans1 is COMMITED:{}",manager.isCommitted(1));
        manager.commit(3);
        log.debug("trans3 is ACTIVE{}",manager.isActive(3));
    }


}
