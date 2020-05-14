package fr.pierrezemb.recordstore.auth;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BiscuitGenerator {
  public static void main(String[] args) {
    BiscuitManager biscuitManager = new BiscuitManager();
    for (String tenant : args) {
      System.out.println(tenant + ": " + biscuitManager.create(tenant, Collections.emptyList()));
    }
  }
}
