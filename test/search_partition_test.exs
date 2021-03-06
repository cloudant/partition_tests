defmodule SearchPartitionTest do
  use CouchTestCase

  @moduledoc """
  Test Partition functionality with search
  """

  def create_docs(db_name, pk1 \\ "foo", pk2 \\ "bar") do
    docs = for i <- 1..10 do
      id = if rem(i, 2) == 0 do 
        "#{pk1}:#{i}" 
      else 
        "#{pk2}:#{i}" 
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
    indexFn = "function(doc) {\n  if (doc.some) {\n    index('some', doc.some);\n }\n}"
    default_ddoc = %{
      indexes: %{
        books: %{
          analyzer: %{name: "standard"},
          index: indexFn
        }
      }
    } 

    ddoc = Enum.into(opts, default_ddoc)

    resp = Couch.put("/#{db_name}/_design/library", body: ddoc)
    assert resp.status_code == 201
    assert Map.has_key?(resp.body, "ok") == true
  end

  def get_ids (resp) do
    %{:body => %{"rows" => rows}} = resp
    Enum.map(rows, fn row -> row["id"] end)
  end

  @tag :with_partitioned_db
  test "Simple query returns partitioned search results", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_partition/foo/_design/library/_search/books"
    resp = Couch.get(url, query: %{q: "some:field"})
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert ids == ["foo:10", "foo:2", "foo:4", "foo:6", "foo:8"]

    url = "/#{db_name}/_partition/bar/_design/library/_search/books"
    resp = Couch.get(url, query: %{q: "some:field"})
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert ids == ["bar:1", "bar:3", "bar:5", "bar:7", "bar:9"]
  end

  @tag :with_partitioned_db
  test "Only returns docs in partition not those in shard", context do
    db_name = context[:db_name]
    create_docs(db_name, "foo", "bar42")
    create_ddoc(db_name)

    url = "/#{db_name}/_partition/foo/_design/library/_search/books"
    resp = Couch.get(url, query: %{q: "some:field"})
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert ids == ["foo:10", "foo:2", "foo:4", "foo:6", "foo:8"]
  end

  @tag :with_partitioned_db
  test "Works with bookmarks and limit", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_partition/foo/_design/library/_search/books"
    resp = Couch.get(url, query: %{q: "some:field", limit: 3})
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert ids == ["foo:10", "foo:2", "foo:4"] 

    %{:body => %{"bookmark" => bookmark}} = resp
    
    resp = Couch.get(url, query: %{q: "some:field", limit: 3, bookmark: bookmark})
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert ids == ["foo:6", "foo:8"]
  end

  @tag :with_partitioned_db
  test "Cannot do global query with partition view", context do 
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_design/library/_search/books"
    resp = Couch.get(url, query: %{q: "some:field"})
    assert resp.status_code == 400
    %{:body => %{"reason" => reason}} = resp
    assert reason == "`partition` parameter is not supported in this search."
  end

  @tag :with_partitioned_db
  test "Cannot do partition query with global search ddoc", context do 
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name, options: %{partitioned: false})

    url = "/#{db_name}/_partition/foo/_design/library/_search/books"
    resp = Couch.get(url, query: %{q: "some:field"})
    assert resp.status_code == 400
    %{:body => %{"reason" => reason}} = resp
    assert reason == "partition query is not supported in this design doc."
  end

  @tag :with_db
  test "normal search on non-partitioned dbs still work", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_design/library/_search/books"
    resp = Couch.get(url, query: %{q: "some:field"})
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert ids == ["bar:1", "bar:5", "bar:9", "foo:2", "bar:3", "foo:4", "foo:6", "bar:7", "foo:8", "foo:10"]
  end

  @tag :with_partitioned_db
  test "All restricted parameters are not allowed", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)
    Enum.each([
        {:include_docs, true}, 
        {:counts, "[\"type\"]"},
        {:group_field, "some"},
        {:ranges, :jiffy.encode(%{price: %{cheap: "[0 TO 100]"}})},
        {:drilldown, "[\"key\",\"a\"]"}
      ], 
      fn ({key, value}) ->
      url = "/#{db_name}/_partition/foo/_design/library/_search/books"
      query =  %{q: "some:field", partition: "foo"}
      query = Map.put(query, key, value)
      resp = Couch.get(url, query: query)
      %{:body => %{"reason" => reason}} = resp
      assert Regex.match?(~r/is not allowed for a partition search/, reason)
    end)
  end

end
