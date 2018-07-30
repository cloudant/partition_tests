defmodule ViewPartitionTest do
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

  def create_ddoc(db_name, opts \\ %{}) do
    mapFn = "function(doc) {\n  if (doc.some) {\n    emit(doc._id, doc.some);\n }\n}"
    default_ddoc = %{
      views: %{
        some: %{
          map: mapFn
        }
      }
    } 

    ddoc = Enum.into(opts, default_ddoc)

    resp = Couch.put("/#{db_name}/_design/mrtest", body: ddoc)
    assert resp.status_code == 201
    assert Map.has_key?(resp.body, "ok") == true
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
  test "query with partitioned:true returns partitioned fields", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name, options: %{partitioned: true})

    url = "/#{db_name}/_design/mrtest/_view/some"
    resp = Couch.get(url, query: %{partition: "foo"})
    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) > 0
    assert_correct_partition(partitions, "foo")

    resp = Couch.get(url, query: %{partition: "bar"})
    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) > 0
    assert_correct_partition(partitions, "bar")
  end

  @tag :with_partitioned_db
  test "default view query returns partitioned fields", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_design/mrtest/_view/some"
    resp = Couch.get(url, query: %{partition: "foo"})
    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) > 0
    assert_correct_partition(partitions, "foo")

    resp = Couch.get(url, query: %{partition: "bar"})
    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) > 0
    assert_correct_partition(partitions, "bar")
  end

  @tag :with_partitioned_db
  test "view query returns all docs for global query", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name, options: %{partitioned: false})

    url = "/#{db_name}/_design/mrtest/_view/some"
    resp = Couch.get(url)
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert length(ids) == 100
  end

  @tag :with_partitioned_db
  test "query errors if illegal partitionkey supplied", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_design/mrtest/_view/some"
    resp = Couch.get(url, query: %{partition: "_bar"})
    assert resp.status_code == 400
  end

  @tag :with_partitioned_db
  test "query works with startkey, endkey range", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_design/mrtest/_view/some"
    resp = Couch.get(url, query: %{partition: "foo", startkey: "1", endkey: "9"})
    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) > 9
    assert_correct_partition(partitions, "foo")
  end

  @tag :with_partitioned_db
  test "query should return 0 results if partition key part of startkey/endkey", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_design/mrtest/_view/some"
    resp = Couch.get(url, body: %{partition: "foo", startkey: "foo:1", endkey: "foo:9"})
    IO.inspect resp
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert length(ids) == 0
  end

  @tag :with_partitioned_db
  test "query works with keys", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_design/mrtest/_view/some"
    resp = Couch.post(url, query: %{partition: "foo"}, body: %{keys: ["2", "4", "6"]})
    IO.inspect resp
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert length(ids) == 3
    assert ids == ["foo:2", "foo:4", "foo:6"]
  end

  @tag :with_partitioned_db
  test "query should return 0 results with partition key in keys", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_design/mrtest/_view/some"
    resp = Couch.post(url, query: %{partition: "bar"}, body: %{keys: ["foo:2", "foo:4", "foo:6"]})
    ids = get_ids(resp)
    assert length(ids) == 0
  end

  @tag :with_partitioned_db
  test "query works with keys in global query", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name, options: %{partitioned: false})

    url = "/#{db_name}/_design/mrtest/_view/some"

    resp = Couch.post(url, body: %{keys: ["foo:2", "foo:4", "foo:6"]})
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert length(ids) == 3
    assert ids == ["foo:2", "foo:4", "foo:6"]
  end

  @tag :with_partitioned_db
  test "query works with limit", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_design/mrtest/_view/some"

    resp = Couch.get(url, query: %{partition: "foo", limit: 5})
    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 5
    assert_correct_partition(partitions, "foo")
  end

  @tag :with_partitioned_db
  test "query works", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_design/mrtest/_view/some"

    resp = Couch.get(url, query: %{partition: "foo", limit: 5})
    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 5
    assert_correct_partition(partitions, "foo")
  end

  @tag :with_partitioned_db
  test "cannot add following to partitioned design doc", context do
    db_name = context[:db_name]

    Enum.each([
      :rewrites, :lists, :shows, :updates, :filters, :validate_doc_update
      ], fn (option) ->
      ddoc = %{} 
      ddoc = Map.put(ddoc, option, "this doesn't need to be valid js for the test")

      resp = Couch.put("/#{db_name}/_design/optionstest", body: ddoc)
      assert resp.status_code == 400
    end)
  end

end
