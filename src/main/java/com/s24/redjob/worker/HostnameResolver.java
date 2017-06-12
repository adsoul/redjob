package com.s24.redjob.worker;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

import org.springframework.util.Assert;

/**
 * Resolve hostname placeholders.
 */
public class HostnameResolver {
   /**
    * Placeholder for hostname without domain.
    */
   public static final String HOSTNAME = "[hostname]";

   /**
    * Placeholder for hostname without domain.
    */
   public static final String FULL_HOSTNAME = "[full-hostname]";

   /**
    * Resolve hostname placeholders {@link #HOSTNAME} and {@link #FULL_HOSTNAME}.
    */
   public static String resolve(String name) throws UnknownHostException {
      Assert.notNull(name, "Precondition violated: name != null.");

      String fullHostname = InetAddress.getLocalHost().getHostName();
      name = name.replaceAll(Pattern.quote(FULL_HOSTNAME), fullHostname);

      int firstDot = fullHostname.indexOf(".");
      String hostname = firstDot > 0? fullHostname.substring(0, firstDot) : fullHostname;
      name = name.replaceAll(Pattern.quote(HOSTNAME), hostname);

      return name;
   }
}
