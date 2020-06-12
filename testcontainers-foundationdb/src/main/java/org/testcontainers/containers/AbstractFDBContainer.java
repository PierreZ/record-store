package org.testcontainers.containers;

/**
 * Use the singleton pattern to have only one FDB started
 * Each tests will use the multi-tenancy feature of the record-store
 */
public abstract class AbstractFDBContainer {
  public static final FoundationDBContainer container;

  static {
    container = new FoundationDBContainer();
    container.start();
  }
}
