defmodule PartitionHelpers do
  use ExUnit.Case

  def create_docs(db_name, pk1 \\ "foo", pk2 \\ "bar") do
    docs = for i <- 1..100 do
      id = if rem(i, 2) == 0 do
        "#{pk1}:#{i}"
      else
        "#{pk2}:#{i}"
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
      mapFn = "function(doc) {\n  if (doc.some) {\n    emit(doc.value, doc.some);\n }\n}"
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

end
