---
title: Capabilities
---

# Query capabilities

Given this schema:

```proto
message User {
  int64 id = 1;
  string name = 2;
  string email = 3;
  repeated string beers = 4;
  string rick_and_morty_quotes = 5;
  map<string, string> favorite_locations_from_tv = 6;
  Address address = 7;
}

message Address {
  string full_address = 1;
  string city = 2;
}
```

Here's a list of the queries you can do(given the right indexes):

Give me the users where:

* id equals `1`,
* id is between `1` and `10`,
* id is `1` or `10`,
* id is `null`
* rick_and_morty_quotes contains `jerry`
* rick_and_morty_quotes contains all `MR MEESEEKS LOOK AT ME`
* beers contains `Trappistes Rochefort 10`
* the map favorite_locations_from_tv has a key starting with `hitchhikers_guide`
* the map favorite_locations_from_tv contains `Earth` has a value
* the nested item address.city equals to `Paris`

The query has a tree-representation, meaning than you can combine many filters.
