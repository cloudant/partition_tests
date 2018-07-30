defmodule AllDocsPartitionTest do
  use CouchTestCase

  @moduledoc """
  Test Partition functionality for all_docs
  """

  def create_docs(db_name) do
    docs = for i <- 1..100 do
      id = if rem(i, 2) == 0 do 
        "foo:#{i}" 
      else 
        "bar:#{i}" 
      end
      %{
        :_id => id,
        :value => i,
        :some => "field"
      }
    end

    resp = Couch.post("/#{db_name}/_bulk_docs", body: %{:docs => docs} )
    assert resp.status_code == 201
  end

  def get_ids(resp) do
    %{:body => %{"rows" => rows}} = resp
    Enum.map(rows, fn row -> row["id"] end)
  end

  def get_partitions(resp) do
    %{:body => %{"rows" => rows}} = resp
    Enum.map(rows, fn row -> 
      [partition, _] = String.split(row["id"], ":")
      partition
    end)
  end

  def assert_correct_partition(partitions, correct_partition) do
    assert Enum.all?(partitions, fn (partition) -> partition == correct_partition end) == true
  end

  @tag :with_partitioned_db
  test "_all_docs query returns all partitioned docs", context do
    db_name = context[:db_name]
    create_docs(db_name)

    url = "/#{db_name}/_all_docs"
    resp = Couch.get(url, query: %{partition: "foo"})
    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) > 0
    assert_correct_partition(partitions, "foo")

    url = "/#{db_name}/_all_docs"
    resp = Couch.get(url, query: %{partition: "bar"})
    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) > 0
    assert_correct_partition(partitions, "bar")
  end

  @tag :with_partitioned_db
  test "_all_docs query returns all docs without partition key supplied", context do
    db_name = context[:db_name]
    create_docs(db_name)

    url = "/#{db_name}/_all_docs"
    resp = Couch.get(url)
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert length(ids) == 100
  end


  @tag :with_partitioned_db
  test "_all_docs errors if illegal partitionkey supplied", context do
    db_name = context[:db_name]
    create_docs(db_name)

    url = "/#{db_name}/_all_docs"
    resp = Couch.get(url, query: %{partition: "_bar"})
    assert resp.status_code == 400
  end

  @tag :with_partitioned_db
  test "_all_docs query works with startkey, endkey range", context do
    db_name = context[:db_name]
    create_docs(db_name)

    url = "/#{db_name}/_all_docs"
    resp = Couch.get(url, query: %{partition: "foo", startkey: "foo:1", endkey: "foo:9"})
    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) > 9
    assert_correct_partition(partitions, "foo")

    resp = Couch.get(url, query: %{partition: "bar", startkey: "foo:1", endkey: "foo:9"})
    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 0
  end

  @tag :with_partitioned_db
  test "_all_docs query works with keys", context do
    db_name = context[:db_name]
    create_docs(db_name)
    url = "/#{db_name}/_all_docs"

    resp = Couch.post(url, query: %{partition: "foo"}, body: %{keys: ["foo:2", "foo:4", "foo:6"]})
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert length(ids) == 3
    assert ids == ["foo:2", "foo:4", "foo:6"]

    resp = Couch.post(url, query: %{partition: "bar"}, body: %{keys: ["foo:2", "foo:4", "foo:6"]})
    ids = get_ids(resp)
    assert length(ids) == 0
  end

  @tag :with_partitioned_db
  test "_all_docs query works with keys in global query", context do
    db_name = context[:db_name]
    create_docs(db_name)
    url = "/#{db_name}/_all_docs"

    resp = Couch.post(url, body: %{keys: ["foo:2", "foo:4", "foo:6"]})
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert length(ids) == 3
    assert ids == ["foo:2", "foo:4", "foo:6"]
  end

  @tag :with_partitioned_db
  test "_all_docs query works with limit", context do
    db_name = context[:db_name]
    create_docs(db_name)
    url = "/#{db_name}/_all_docs"

    resp = Couch.get(url, query: %{partition: "foo", limit: 5})
    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 5
    assert_correct_partition(partitions, "foo")
  end

end
