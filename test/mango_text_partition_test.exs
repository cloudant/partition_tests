defmodule MangoTextPartitionTest do
  use CouchTestCase
  import PartitionHelpers, except: [get_partitions: 1]


#   {
#     "index": {
#         "fields": [
#             {
#                 "name": "Movie_name",
#                 "type": "string"
#             }
#         ]
#     },
#     "name": "Movie_name-text",
#     "type": "text"
# }

  @moduledoc """
  Test Partition functionality for mango text
  """
  def create_index(db_name, opts \\ %{}) do
    default_index = %{
      index: %{
        fields: [
          %{
            "name": "some",
            "type": "string"
          }
        ]
      },
      type: "text"
    }

    index = Enum.into(opts, default_index)
    resp = Couch.post("/#{db_name}/_index", body: index)

    assert resp.status_code == 200
    assert resp.body["result"] == "created"
  end

  def get_partitions(resp) do
    %{:body => %{"docs" => docs}} = resp
    Enum.map(docs, fn doc ->
      [partition, _] = String.split(doc["_id"], ":")
      partition
    end)
  end

  @tag :with_partitioned_db
  test "partitioned query returns partitioned results", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_index(db_name)

    url = "/#{db_name}/_partition/foo/_explain"
    resp = Couch.post(url, body: %{
      selector: %{
        some: "field"
      },
    })

    %{:body => body} = resp
    assert body["index"]["def"]["fields"] == [%{"some" => "string"}]

    url = "/#{db_name}/_partition/foo/_find"
    resp = Couch.post(url, body: %{
      selector: %{
        some: "field"
      },
      limit: 20
    })

    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 20
    assert_correct_partition(partitions, "foo")

    url = "/#{db_name}/_partition/bar/_find"
    resp = Couch.post(url, body: %{
      selector: %{
        some: "field"
      },
      limit: 20
    })

    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 20
    assert_correct_partition(partitions, "bar")
  end

  @tag :with_partitioned_db
  test "partitioned db with global query returns results", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_index(db_name, %{partitioned: false})

    url = "/#{db_name}/_explain"
    resp = Couch.post(url, body: %{
      selector: %{
        some: "field"
      },
    })

    %{:body => body} = resp
    assert body["index"]["def"]["fields"] == [%{"some" => "string"}]

    url = "/#{db_name}/_find"
    resp = Couch.post(url, body: %{
      selector: %{
        some: "field"
      },
    })

    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 100
  end

  @tag :with_partitioned_db
  test "partitioned database global query does not use partition index", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_index(db_name)

    url = "/#{db_name}/_explain"
    selector = %{
      selector: %{
        some: "field"
      }
    }

    resp = Couch.post(url, body: selector)
    %{:body => body} = resp
    assert body["index"]["name"] == "_all_docs"

    url = "/#{db_name}/_find"
    resp = Couch.post(url, body: selector)

    assert resp.status_code == 200

    partitions = get_partitions(resp)
    assert length(partitions) == 100
  end

  @tag :with_partitioned_db
  test "partitioned database partitioned query does not use global index", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_index(db_name, %{partitioned: false})

    url = "/#{db_name}/_partition/foo/_explain"
    selector = %{
      selector: %{
        some: "field"
      }
    }

    resp = Couch.post(url, body: selector)
    assert resp.status_code == 200
    %{:body => body} = resp
    assert body["index"]["name"] == "_all_docs"

    url = "/#{db_name}/_partition/foo/_find"
    resp = Couch.post(url, body: selector)
    assert resp.status_code == 200

    partitions = get_partitions(resp)
    assert length(partitions) == 50
    assert_correct_partition(partitions, "foo")
  end
end
