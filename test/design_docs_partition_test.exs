defmodule DesignDocPartitionTest do
  use CouchTestCase

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

    fake_section = %{section: ""}

    Enum.each([
      {:shows, fake_section},
      {:lists, fake_section},
      {:rewrites, ""},
      {:updates, fake_section},
      {:filters, fake_section},
      {:validate_doc_update, ""}
      ], fn ({option, value}) ->
      ddoc = %{}
      ddoc = Map.put(ddoc, option, value)

      resp = Couch.put("/#{db_name}/_design/optionstest", body: ddoc)
      assert resp.status_code == 400
      %{:body => %{"reason" => reason}} = resp
      assert reason == "`#{option}` cannot be used in a partitioned design doc"
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

    error = %{
      "error" => "invalid_design_doc",
      "reason" => "Javascript reduces not supported in partitioned view."
    }

    assert resp.status_code == 400
    assert Map.get(resp, :body) == error
  end
end
