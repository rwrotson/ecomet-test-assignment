CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE test.repositories
(
    name     String,
    owner    String,
    stars    Int32,
    watchers Int32,
    forks    Int32,
    language String,
    updated  DateTime
) ENGINE = ReplacingMergeTree(updated)
      ORDER BY name;

CREATE TABLE test.repositories_authors_commits
(
    date        Date,
    repo        String,
    author      String,
    commits_num Int32
) ENGINE = ReplacingMergeTree
      ORDER BY (date, repo, author);

CREATE TABLE test.repositories_positions
(
    date     Date,
    repo     String,
    position UInt32
) ENGINE = ReplacingMergeTree
      ORDER BY (date, repo);