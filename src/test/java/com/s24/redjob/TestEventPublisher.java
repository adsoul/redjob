package com.s24.redjob;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;

/**
 * {@link ApplicationEventPublisher} that allows to wait for events. Useful for tests.
 */
public class TestEventPublisher implements ApplicationEventPublisher {
   /**
    * Logger.
    */
   private static final Logger log = LoggerFactory.getLogger(TestEventPublisher.class);

   /**
    * All recorded events.
    */
   private final List<Object> events = Collections.synchronizedList(new ArrayList<>());

   /**
    * Most recent event.
    */
   private volatile BlockingQueue<Object> recent = new LinkedBlockingQueue<>(1);

   @Override
   public void publishEvent(ApplicationEvent event) {
      doPublishEvent(event);
   }

   @Override
   public void publishEvent(Object event) {
      doPublishEvent(event);
   }

   /**
    * Record event. Blocks, if last event has not yet been consumed by {@link #waitForEvent(long, TimeUnit)}.
    *
    * @param event
    *           Event.
    */
   protected void doPublishEvent(Object event) {
      log.info("Publish {}.", event.getClass().getSimpleName());
      events.add(event);
      try {
         if (!recent.offer(event, 1, TimeUnit.SECONDS)) {
            throw new IllegalArgumentException("Missing wait for prior event " + recent.peek().getClass().getSimpleName() + ".");
         }
      } catch (InterruptedException e) {
         // Ignore
         Thread.currentThread().interrupt();
      }
   }

   /**
    * All events.
    */
   public List<Object> getEvents() {
      return events;
   }

   /**
    * Wait one second for event.
    *
    * @return Event, or null, if no event has been published until the timeout.
    */
   public Object waitForEvent() throws InterruptedException {
      Object event = waitForEvent(1, TimeUnit.SECONDS);
      log.info("Consumed {}.", event.getClass().getSimpleName());
      return event;
   }

   /**
    * Wait for event.
    *
    * @param timeout
    *           Timeout.
    * @param unit
    *           Time unit of timeout.
    * @return Event, or null, if no event has been published until the timeout.
    */
   public Object waitForEvent(long timeout, TimeUnit unit) throws InterruptedException {
      return recent.poll(timeout, unit);
   }

   /**
    * Stop blocking in {@link #doPublishEvent(Object)}.
    */
   public void doNotBlock() {
      recent = new LinkedBlockingDeque<>(recent);
   }
}
