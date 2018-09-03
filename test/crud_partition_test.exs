defmodule CrudPartitionTest do
  use CouchTestCase

  @tag :with_partitioned_db
  test "Sets partition in db info", context do
    db_name = context[:db_name]
    resp = Couch.get("/#{db_name}")
    %{body: body} = resp
    assert body["props"] == %{"partitioned" => true}
  end

  @tag :with_partitioned_db
  test "PUT and GET document", context do
    db_name = context[:db_name]
    id = "my-partition:doc"
    url = "/#{db_name}/#{id}"

    resp = Couch.put(url, body: %{partitioned_doc: true})
    assert resp.status_code == 201

    resp = Couch.get(url)
    assert resp.status_code == 200

    %{body: doc} = resp
    assert doc["_id"] == id
  end

  @tag :with_partitioned_db
  test "PUT fails if a partition key is not supplied", context do
    db_name = context[:db_name]
    id = "not-partitioned"
    url = "/#{db_name}/#{id}"

    resp = Couch.put(url, body: %{partitioned_doc: false})
    assert resp.status_code == 400
  end

  @tag :with_partitioned_db
  test "POST and GET document", context do
    db_name = context[:db_name]
    id = "my-partition-post:doc"
    url = "/#{db_name}"

    resp = Couch.post(url, body: %{_id: id, partitioned_doc: true})
    assert resp.status_code == 201

    resp = Couch.get("#{url}/#{id}")
    assert resp.status_code == 200

    %{body: doc} = resp
    assert doc["_id"] == id
  end

  @tag :with_partitioned_db
  test "POST fails if a partition key is not supplied", context do
    db_name = context[:db_name]
    id = "not-partitioned-post"
    url = "/#{db_name}"

    resp = Couch.post(url, body: %{_id: id, partitited_doc: false})
    assert resp.status_code == 400
  end

  @tag :with_partitioned_db
  test "_BULK_DOCS saves docs with partition key", context do
    db_name = context[:db_name]
    docs = [
      %{_id: "foo:1"},
      %{_id: "bar:1"},
    ]

    url = "/#{db_name}"
    resp = Couch.post("#{url}/_bulk_docs", body: %{:docs => docs} )
    assert resp.status_code == 201

    resp = Couch.get("#{url}/foo:1")
    assert resp.status_code == 200

    resp = Couch.get("#{url}/bar:1")
    assert resp.status_code == 200
  end

  @tag :with_partitioned_db
  test "_BULK_DOCS errors with missing partition key", context do
    db_name = context[:db_name]
    docs = [
      %{_id: "foo1"},
    ]

    error = %{
      "error" => "illegal_docid",
      "reason" => "doc id must be of form partition:id"
    }

    url = "/#{db_name}"
    resp = Couch.post("#{url}/_bulk_docs", body: %{:docs => docs} )
    assert resp.status_code == 400
    assert Map.get(resp, :body) == error
  end

  @tag :with_partitioned_db
  test "_BULK_DOCS errors with bad partition key", context do
    db_name = context[:db_name]
    docs = [
      %{_id: "_foo:1"},
    ]

    error = %{
      "error" => "illegal_docid", 
      "reason" => "Only reserved document ids may start with underscore."
    }

    url = "/#{db_name}"
    resp = Couch.post("#{url}/_bulk_docs", body: %{:docs => docs} )
    assert resp.status_code == 400
    assert Map.get(resp, :body) == error
  end

  @tag :with_partitioned_db
  test "_BULK_DOCS errors with bad doc key", context do
    db_name = context[:db_name]
    docs = [
      %{_id: "foo:"},
    ]

    error = %{
      error: "illegal_doc_key",
      reason: "document key must be specified",
    }

    url = "/#{db_name}"
    resp = Couch.post("#{url}/_bulk_docs", body: %{:docs => docs} )
    assert resp.status_code == 400
    assert Map.get(resp, :body) == error
  end

  @tag :with_partitioned_db
  test "saves attachment with partitioned doc", context do
    db_name = context[:db_name]
    id = "foo:doc-with-attachment"

    doc = %{
      _id: id,
      _attachments: %{
        "foo.txt": %{
          content_type: "text/plain",
          data: Base.encode64("This is a text document to save")
        }
      }
    }
    resp = Couch.put("/#{db_name}/#{id}", [body: doc])

    assert resp.status_code == 201

    resp = Couch.get("/#{db_name}/#{id}")
    assert resp.status_code == 200
    body = Map.get(resp, :body)
    rev = Map.get(body, "_rev")

    assert body["_attachments"] == %{
      "foo.txt" => %{
        "content_type" => "text/plain",
        "digest" => "md5-OW2BoZAtMqs1E+fAnLpNBw==",
        "length" => 31,
        "revpos" => 1,
        "stub" => true
      }
    }

    resp = Couch.get("/#{db_name}/#{id}/foo.txt")
    assert Map.get(resp, :body)  == "This is a text document to save"

    resp = Couch.put("/#{db_name}/#{id}/bar.txt?rev=#{rev}",[
      headers: ["Content-Type": "text/plain"],
      body: "This is another document"
    ])

    assert resp.status_code == 200
  end

  test "Create database with bad `partitioned` value", context do
    resp = Couch.put("/bad-db?partitioned=tru")
    assert resp.status_code == 400
    assert Map.get(resp, :body) == %{
      "error" => "bad_request",
      "reason" => "`partitioned` parameter can only be set to true."
    }
  end

  test "Cannot create partitioned system db", context do
    Couch.delete("/_replicator")

    resp = Couch.put("/_replicator?partitioned=true")
    assert resp.status_code == 400

    %{:body => %{"reason" => reason}} = resp
    assert Regex.match?(~r/Cannot partition a system database/, reason)
  end
end
