package com.s24.redjob.queue;

import java.util.List;

import org.springframework.beans.factory.FactoryBean;

import com.s24.redjob.worker.AbstractWorkerFactoryBean;

/**
 * {@link FactoryBean} for easy creation of a {@link FifoWorker}.
 */
public class FifoWorkerFactoryBean extends AbstractWorkerFactoryBean<FifoWorker> {
   /**
    * Queue dao.
    */
   private FifoDaoImpl fifoDao;

   /**
    * Constructor.
    */
   public FifoWorkerFactoryBean() {
      super(new FifoWorker());
   }

   @Override
   public void afterPropertiesSet() throws Exception {
      worker.setFifoDao(fifoDao);

      super.afterPropertiesSet();
   }

   //
   // Injections.
   //

   /**
    * Queue dao.
    */
   public FifoDaoImpl getFifoDao() {
      return fifoDao;
   }

   /**
    * Queue dao.
    */
   public void setFifoDao(FifoDaoImpl fifoDao) {
      this.fifoDao = fifoDao;
   }

   /**
    * Queues to listen to.
    */
   public List<String> getQueues() {
      return worker.getQueues();
   }

   /**
    * Queues to listen to.
    */
   public void setQueues(String... queues) {
      worker.setQueues(queues);
   }

   /**
    * Queues to listen to.
    */
   public void setQueues(List<String> queues) {
      worker.setQueues(queues);
   }
}
