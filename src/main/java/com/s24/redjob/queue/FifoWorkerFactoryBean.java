package com.s24.redjob.queue;

import com.s24.redjob.worker.AbstractWorkerFactoryBean;

import java.util.List;

import org.springframework.beans.factory.FactoryBean;

/**
 * {@link FactoryBean} for easy creation of a {@link FifoWorker}.
 */
public class FifoWorkerFactoryBean extends AbstractWorkerFactoryBean<FifoWorker> {
   /**
    * Queue dao.
    */
   private FifoDaoImpl fifoDao;

   /**
    * Should worker start paused?. Defaults to false.
    */
   private boolean startPaused = false;

   /**
    * Constructor.
    */
   public FifoWorkerFactoryBean() {
      super(new FifoWorker());
   }

   @Override
   public void afterPropertiesSet() throws Exception {
      worker.setFifoDao(fifoDao);
      worker.pause(startPaused);

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

   /**
    * Should worker start paused?. Defaults to false.
    */
   public boolean isStartPaused() {
      return startPaused;
   }

   /**
    * Should worker start paused?. Defaults to false.
    */
   public void setStartPaused(boolean startPaused) {
      this.startPaused = startPaused;
   }
}
