# Elixir Test Suite

Elixir tests for the new partition work

To run the suite:

```bash
./run
```

## Tips for test

* `mix test` runs all the tests
* `mix test ./test/view_partition_test.exs` runs only the tests in that file
* `mix test ./test/view_partition_test.exs:174` runs the test starting at line 174

# Tests to add

* Responses for non-existent partitions are 404?
* Replication tests 
