package fr.pierrezemb.recordstore.client;

public class RecordStoreClient {
  private final String tenant;
  private final String container;
  private final String address;
  private final String token;

  private RecordStoreClient(String tenant, String container, String address, String token) {
    this.tenant = tenant;
    this.container = container;
    this.address = address;
    this.token = token;
  }

  public static class Builder {

    private String tenant;
    private String container;
    private String address;
    private String token;

    public Builder withTenant(String tenant) {
      this.tenant = tenant;
      return this;
    }

    public Builder withContainer(String container) {
      this.container = container;
      return this;
    }

    public Builder withAddress(String address) {
      this.address = address;
      return this;
    }

    public Builder withToken(String token) {
      this.token = token;
      return this;
    }

    public RecordStoreClient build() {
      return new RecordStoreClient(tenant, container, address, token);
    }
  }
}
