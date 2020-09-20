---
title: Motivations
---

# Motivations

## From developers

* Handling data is hard
* "I don't want to create another table just to store this" should not be an excuse, I should be able to create tables whenever I want
* handling Protobufs is easier than a SQL table and I can reuse it for serde operations
* key-value stores are great, but it takes time to create value on top of it
* I need transactions to be consistent over multiples writes
* we should use something with strong capabilities:
    * [exhibit A from MongoDB](https://jepsen.io/analyses/mongodb-4.2.6)
    * [exhibit B from ETCD](https://jepsen.io/analyses/etcd-3.4.3)
    * or [any Jepsen's analysis](http://jepsen.io/analyses)

## From operators

* Handling data is hard,
* non-distributed databases are hard to manage,
* distributed systems are also hard but offer greater capabilities,
* when we have a massive distributed key-value store, we should be able to dedicate a keyspace to a user and a context,
