defmodule MangoPartitionTest do
  use CouchTestCase

  @moduledoc """
  Test Partition functionality for mango
  """
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

  def create_index(db_name, fields \\ ["some"]) do
    resp = Couch.post("/#{db_name}/_index", body: %{
      index: %{
        fields: fields
      }
    })

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

  def assert_correct_partition(partitions, correct_partition) do
    assert Enum.all?(partitions, fn (partition) -> partition == correct_partition end) == true
  end

  @tag :with_partitioned_db
  test "query with partitioned:true using index and $eq", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_index(db_name)

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
  test "partition query with partition in body returns 400", context do
    db_name = context[:db_name]

    url = "/#{db_name}/_partition/foo/_find"
    resp = Couch.post(url, body: %{
      partition: "foo",
      selector: %{
        some: "field"
      },
      limit: 20
    })

    assert resp.status_code == 400
    %{:body => %{"reason" => reason}} = resp
    assert Regex.match?(~r/`partition` is not a valid parameter./, reason)
  end

  @tag :with_partitioned_db
  test "global query with partition in body returns 400", context do
    db_name = context[:db_name]

    url = "/#{db_name}/_find"
    resp = Couch.post(url, body: %{
      partition: "foo",
      selector: %{
        some: "field"
      },
      limit: 20
    })

    assert resp.status_code == 400
    %{:body => %{"reason" => reason}} = resp
    assert Regex.match?(~r/`partition` is not a valid parameter./, reason)
  end

  @tag :with_partitioned_db
  test "partitioned database query using _all_docs with $eq", context do
    db_name = context[:db_name]
    create_docs(db_name)

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

  @tag :with_db
  test "non-partitioned database query using _all_docs and $eq", context do
    db_name = context[:db_name]
    create_docs(db_name)

    url = "/#{db_name}/_find"
    resp = Couch.post(url, body: %{
      selector: %{
        some: "field"
      },
      limit: 5
    })

    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 5
    assert partitions == ["bar", "foo", "bar", "foo", "bar"]
  end

  @tag :with_partitioned_db
  test "partitioned database query using index and range scan", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_index(db_name, ["value"])

    url = "/#{db_name}/_partition/foo/_find"
    resp = Couch.post(url, body: %{
      selector: %{
        value: %{
          "$gte": 6,
          "$lt": 16
        }
      }
    })

    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 5
    assert_correct_partition(partitions, "foo")

    url = "/#{db_name}/_partition/bar/_find"
    resp = Couch.post(url, body: %{
      selector: %{
        value: %{
          "$gte": 6,
          "$lt": 16
        }
      }
    })

    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 5
    assert_correct_partition(partitions, "bar")
  end 

  @tag :with_partitioned_db
  test "partitioned database query using index and range scan all docs on same shard", context do
    db_name = context[:db_name]
    create_docs(db_name, "foo", "bar42")
    create_index(db_name, ["value"])

    url = "/#{db_name}/_partition/foo/_find"
    resp = Couch.post(url, body: %{
      selector: %{
        value: %{
          "$gte": 6,
          "$lt": 16
        }
      }
    })

    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 5
    assert_correct_partition(partitions, "foo")

    url = "/#{db_name}/_partition/bar42/_find"
    resp = Couch.post(url, body: %{
      selector: %{
        value: %{
          "$gte": 6,
          "$lt": 16
        }
      }
    })

    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 5
    assert_correct_partition(partitions, "bar42")
  end 

  @tag :with_partitioned_db
  test "partitioned database query using _all_docs and range scan", context do
    db_name = context[:db_name]
    create_docs(db_name)

    url = "/#{db_name}/_partition/foo/_find"
    resp = Couch.post(url, body: %{
      selector: %{
        value: %{
          "$gte": 6,
          "$lt": 16
        }
      }
    })

    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 5
    assert_correct_partition(partitions, "foo")

    url = "/#{db_name}/_partition/bar/_find"
    resp = Couch.post(url, body: %{
      selector: %{
        value: %{
          "$gte": 6,
          "$lt": 16
        }
      }
    })

    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 5
    assert_correct_partition(partitions, "bar")
  end

  @tag :with_partitioned_db
  test "partitioned database query using _all_docs and range scan all docs on same shard", context do
    db_name = context[:db_name]
    create_docs(db_name, "foo", "bar42")

    url = "/#{db_name}/_partition/foo/_find"
    resp = Couch.post(url, body: %{
      selector: %{
        value: %{
          "$gte": 6,
          "$lt": 16
        }
      }
    })

    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 5
    assert_correct_partition(partitions, "foo")

    url = "/#{db_name}/_partition/bar42/_find"
    resp = Couch.post(url, body: %{
      selector: %{
        value: %{
          "$gte": 6,
          "$lt": 16
        }
      }
    })

    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 5
    assert_correct_partition(partitions, "bar42")
  end


  @tag :with_partitioned_db
  test "explain works with partitions", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_index(db_name, ["some"])

    url = "/#{db_name}/_partition/foo/_explain"
    resp = Couch.post(url, body: %{
      selector: %{
        value: %{
          "$gte": 6,
          "$lt": 16
        }
      }
    })

    %{:body => body} = resp

    assert body["index"]["name"] == "_all_docs"
    assert body["mrargs"]["extra"]["partition"] == "foo"

    url = "/#{db_name}/_partition/bar/_explain"
    resp = Couch.post(url, body: %{
      selector: %{
        some: "field"
      }
    })

    %{:body => body} = resp

    assert body["index"]["def"] == %{"fields" => [%{"some" => "asc"}]}
    assert body["mrargs"]["extra"]["partition"] == "bar"
  end

  @tag :with_db
  test "explain works with non partitioned db", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_index(db_name, ["some"])

    url = "/#{db_name}/_explain"
    resp = Couch.post(url, body: %{
      selector: %{
        value: %{
          "$gte": 6,
          "$lt": 16
        }
      }
    })

    %{:body => body} = resp

    assert body["index"]["name"] == "_all_docs"
    assert Map.has_key?(body["mrargs"]["extra"], "partition") == false

    resp = Couch.post(url, body: %{
      selector: %{
        some: "field"
      }
    })

    %{:body => body} = resp

    assert body["index"]["def"] == %{"fields" => [%{"some" => "asc"}]}
    assert Map.has_key?(body["mrargs"]["extra"], "partition") == false
  end

  @tag :with_partitioned_db
  test "partitioned database query using bookmarks", context do
    db_name = context[:db_name]
    create_docs(db_name)
    create_index(db_name, ["value"])

    url = "/#{db_name}/_partition/foo/_find"
    resp = Couch.post(url, body: %{
      selector: %{
        value: %{
          "$gte": 6,
          "$lt": 16
        }
      },
      limit: 3
    })

    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 3
    assert_correct_partition(partitions, "foo")

    %{:body => %{"bookmark" => bookmark}} = resp

    resp = Couch.post(url, body: %{
      selector: %{
        value: %{
          "$gte": 6,
          "$lt": 16
        }
      },
      limit: 3,
      bookmark: bookmark
    })

    assert resp.status_code == 200
    partitions = get_partitions(resp)
    assert length(partitions) == 2
    assert_correct_partition(partitions, "foo")
  end

  @tag :with_partitioned_db
  test "partition database query with r = 3 is rejected", context do
    db_name = context[:db_name]
    url = "/#{db_name}/_partition/foo/_find"
    resp = Couch.post(url, body: %{
      selector: %{
        value: %{
          "$gte": 6,
          "$lt": 16
        }
      },
      r: 3
    })

    assert resp.status_code == 400
  end

  @tag :with_db
  test "global db query with r = 3 is accepted", context do
    db_name = context[:db_name]
    url = "/#{db_name}/_find"
    resp = Couch.post(url, body: %{
      selector: %{
        value: %{
          "$gte": 6,
          "$lt": 16
        }
      },
      r: 3
    })

    assert resp.status_code == 200
  end

  # Todo:
  # Add partition true/false to creating index
end