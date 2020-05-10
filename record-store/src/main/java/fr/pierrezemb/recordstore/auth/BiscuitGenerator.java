package fr.pierrezemb.recordstore.auth;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BiscuitGenerator {
  public static void main(String[] args) {
    BiscuitManager biscuitManager = new BiscuitManager();

    List<String> users = Arrays.asList("pierre", "steven");
    for (String user : users) {
      System.out.println(user + ": " + biscuitManager.create(user, Collections.emptyList()));
    }
  }
}
