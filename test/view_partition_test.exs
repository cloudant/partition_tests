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

      group = if rem(i, 3) == 0 do
          "one"
        else
          "two"
      end

      %{
        :_id => id,
        :value => i,
        :some => "field",
        :group => group
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

  def create_reduce_ddoc(db_name, opts \\ %{}) do
    mapFn = "function(doc) {\n  if (doc.group) {\n    emit([doc.some, doc.group], 1);\n }\n}"
    default_ddoc = %{
      views: %{
        some: %{
          map: mapFn,
          reduce: "_count"
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

  def get_reduce_result (resp) do
    %{:body => %{"rows" => rows}} = resp
    rows
  end

  def assert_correct_partition(partitions, correct_partition) do
    assert Enum.all?(partitions, fn (partition) -> partition == correct_partition end) == true
  end

  @tag :with_partitioned_db
  test "query with partitioned:true returns partitioned fields", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name, options: %{partitioned: true})

    url = "/#{db_name}/_partition/foo/_design/mrtest/_view/some"
    resp = Couch.get(url)
    IO.inspect resp
    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) > 0
    assert_correct_partition(partitions, "foo")

    url = "/#{db_name}/_partition/bar/_design/mrtest/_view/some"
    resp = Couch.get(url)
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

    url = "/#{db_name}/_partition/foo/_design/mrtest/_view/some"
    resp = Couch.get(url)
    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) > 0
    assert_correct_partition(partitions, "foo")

    url = "/#{db_name}/_partition/bar/_design/mrtest/_view/some"
    resp = Couch.get(url)
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
  test "query errors with in correct partitionkeys supplied", context do
    db_name = context[:db_name]
    create_ddoc(db_name)

    url = "/#{db_name}/_partition/_bar/_design/mrtest/_view/some"
    resp = Couch.get(url)
    assert resp.status_code == 400

    url = "/#{db_name}/_partition//_design/mrtest/_view/some"
    resp = Couch.get(url)
    assert resp.status_code == 400

    url = "/#{db_name}/_partition/%20/_design/mrtest/_view/some"
    resp = Couch.get(url)
    assert resp.status_code == 400
  end

  @tag :with_partitioned_db
  test "query errors if restricted params used", context do
    db_name = context[:db_name]
    create_ddoc(db_name)

    Enum.each([
      {:include_docs, true}, 
      {:conflicts, true},
      {:stable, true}
    ], 
      fn ({key, value}) ->
      query =  %{}
      query = Map.put(query, key, value)
      url = "/#{db_name}/_partition/foo/_design/mrtest/_view/some"
      resp = Couch.get(url, query: query)
      assert resp.status_code == 400
      %{:body => body} = Couch.get(url, query: query)
      assert body["reason"] === "#{key} is not allowed for a partition query"

    end)
  end

  @tag :with_partitioned_db
  test "query works with startkey, endkey range", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_partition/foo/_design/mrtest/_view/some"
    resp = Couch.get(url, query: %{startkey: "1", endkey: "9"})
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

    url = "/#{db_name}/_partition/foo/_design/mrtest/_view/some"
    resp = Couch.get(url, body: %{startkey: "foo:1", endkey: "foo:9"})
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert length(ids) == 0
  end

  @tag :with_partitioned_db
  test "query works with keys", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_partition/foo/_design/mrtest/_view/some"
    resp = Couch.post(url, body: %{keys: ["2", "4", "6"]})
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

    url = "/#{db_name}/_partition/foo/_design/mrtest/_view/some"
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

    url = "/#{db_name}/_partition/foo/_design/mrtest/_view/some"

    resp = Couch.get(url, query: %{limit: 5, partition: "foo"})
    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 5
    assert_correct_partition(partitions, "foo")
  end

  @tag :with_partitioned_db
  test "query with partition in query string fails", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_partition/foo/_design/mrtest/_view/some"

    resp = Couch.get(url, query: %{partition: "foo"})
    assert resp.status_code == 400
  end

  @tag :with_partitioned_db
  test "query with descending works", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_partition/foo/_design/mrtest/_view/some"

    resp = Couch.get(url, query: %{descending: true, limit: 5})
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert length(ids) == 5
    assert ids == ["foo:98", "foo:96", "foo:94", "foo:92", "foo:90"]

    resp = Couch.get(url, query: %{descending: false, limit: 5})
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert length(ids) == 5
    assert ids == ["foo:10", "foo:100", "foo:12", "foo:14", "foo:16"]
  end

  @tag :with_partitioned_db
  test "query with skip works", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_partition/foo/_design/mrtest/_view/some"

    resp = Couch.get(url, query: %{skip: 5, limit: 5})
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert length(ids) == 5
    assert ids == ["foo:18", "foo:2", "foo:20", "foo:22", "foo:24"]
  end

  @tag :with_partitioned_db
  test "query with key works", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_partition/foo/_design/mrtest/_view/some"

    resp = Couch.get(url, query: %{key: "22"})
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert length(ids) == 1
    assert ids == ["foo:22"]
  end

  @tag :with_partitioned_db
  test "query with startkey_docid and endkey_docid", context do
    mapFn = "function(doc) {\n  if (doc.some) {\n    emit(doc.some);\n }\n}"
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name, views: %{
      some: %{
        map: mapFn
      }
    })

    url = "/#{db_name}/_partition/foo/_design/mrtest/_view/some"

    resp = Couch.get(url, query: %{
      startkey: "\"field\"",
      endkey: "\"field\"",
      startkey_docid: "18",
      endkey_docid: "24"
    })
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert ids == ["foo:18", "foo:2", "foo:20", "foo:22", "foo:24"]
  end

  @tag :with_partitioned_db
  test "query with update=false works", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_partition/foo/_design/mrtest/_view/some"
    resp = Couch.get(url, query: %{
      update: "true",
      limit: 3
    })
    assert resp.status_code == 200

    resp = Couch.put("/#{db_name}/foo:1", body: %{some: "field"})

    resp = Couch.get(url, query: %{
      update: "false",
      limit: 3
    })
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert ids == ["foo:10", "foo:100", "foo:12"]
  end

  @tag :with_partitioned_db
  test "query with reduce works", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_reduce_ddoc(db_name)

    url = "/#{db_name}/_partition/foo/_design/mrtest/_view/some"
    resp = Couch.get(url, query: %{
      reduce: true,
      group_level: 1
    })

    assert resp.status_code == 200
    results = get_reduce_result(resp)
    assert results ==  [%{"key" => ["field"], "value" => 100}]

    resp = Couch.get(url, query: %{
      partition: "foo",
      reduce: true,
      group_level: 2
    })

    assert results = [
      %{"key" => ["field", "one"], "value" => 33},
      %{"key" => ["field", "two"], "value" => 67}
    ]

    resp = Couch.get(url, query: %{
      partition: "foo",
      reduce: true,
      group: true
    })

    assert results = [
      %{"key" => ["field", "one"], "value" => 33},
      %{"key" => ["field", "two"], "value" => 67}
    ]
  end
end
