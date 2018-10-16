

couch:
	mix test --trace ./test/mango_partition_test.exs \
								 ./test/view_partition_test.exs \
								 ./test/design_docs_partition_test.exs \
								 ./test/crud_partition_test.exs \
								 ./test/partition_size_test.exs

all:
	mix test --trace
