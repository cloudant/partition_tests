defmodule DesignDocPartitionTest do
  use CouchTestCase
  import PartitionHelpers

  @moduledoc """
  Test Partition functionality for partition design docs
  """

  @tag :with_partitioned_db
  test "/_partition/:pk/_design/doc 404", context do
    db_name = context[:db_name]

    url = "/#{db_name}/_partition/fake-key/_design/mrtest/"
    resp = Couch.get(url)
    assert resp.status_code == 404
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

  
  @tag :with_partitioned_db
  test "cannot add a js reduce to a partitioned design doc", context do
    db_name = context[:db_name]
    mapFn = "function(doc) {\n  if (doc.some) {\n    emit(doc._id, doc.some);\n }\n}"
    reduceFn = "function(keys, values) { return sum(values); }"
    ddoc = %{
      views: %{
        some: %{
          map: mapFn,
          reduce: reduceFn
        }
      }
    } 

    resp = Couch.put("/#{db_name}/_design/mrtest", body: ddoc)
    assert resp.status_code == 400
  end
end
