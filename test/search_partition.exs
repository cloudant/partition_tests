defmodule SearchPartitionTest do
  use CouchTestCase

  @moduledoc """
  Test Partition functionality with search
  """

  def create_docs(db_name) do
    docs = for i <- 1..10 do
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

  def create_ddoc(db_name) do
    indexFn = "function(doc) {\n  if (doc.some) {\n    index('some', doc.some);\n }\n}"
    ddoc = %{
      indexes: %{
        books: %{
          analyzer: %{name: "standard"},
          index: indexFn
        }
      }
    } 

    resp = Couch.put("/#{db_name}/_design/library", body: ddoc)
    assert resp.status_code == 201
    assert Map.has_key?(resp.body, "ok") == true
  end

  def get_ids (resp) do
    %{:body => %{"rows" => rows}} = resp
    Enum.map(rows, fn row -> row["id"] end)
  end

  @tag :with_partition_db
  test "Simple query returns partitioned search results", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)

    url = "/#{db_name}/_design/library/_search/books"
    resp = Couch.get(url, query: %{q: "some:field", partition: "foo"})
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert ids == ["foo:10", "foo:2", "foo:4", "foo:6", "foo:8"]

    url = "/#{db_name}/_design/library/_search/books"
    resp = Couch.get(url, query: %{q: "some:field", partition: "bar"})
    assert resp.status_code == 200
    ids = get_ids(resp)
    assert ids == ["bar:1", "bar:3", "bar:5", "bar:7", "bar:9"]
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

  @tag :with_partition_db
  test "All restricted paramters are not allowed", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_ddoc(db_name)
    Enum.each([
        {:include_docs, true}, 
        {:counts, "[\"type\"]"}
      ], 
      fn ({key, value}) ->
      url = "/#{db_name}/_design/library/_search/books"
      query =  %{q: "some:field", partition: "foo"}
      query = Map.put(query, key, value)
      %{:body => body} = Couch.get(url, query: query)
      assert body["error"] === "search_partition_error", "Failed for #{key}: #{value}"

    end)
  end

end
