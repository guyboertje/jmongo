require './test/test_helper'
require 'logger'

Cfg.connection :op_timeout => 10
Cfg.db

class CursorTest < MiniTest::Unit::TestCase
  include Mongo

  def setup
    Cfg.clear_all
    #Cfg.coll.insert('a' => 1)     # collection not created until it's used
  end

  def test_alive
    batch = []
    5000.times do |n|
      batch << {:a => n}
    end

    Cfg.coll.insert(batch)
    cursor = Cfg.coll.find
    assert !cursor.alive?
    cursor.next
    assert cursor.alive?
    cursor.close
    assert !cursor.alive?
  end

  def test_add_and_remove_options
    Cfg.coll.insert('a' => 1)
    c = Cfg.coll.find
    assert_equal 0, c.options & OP_QUERY_EXHAUST
    c.add_option(OP_QUERY_EXHAUST)
    assert_equal OP_QUERY_EXHAUST, c.options & OP_QUERY_EXHAUST
    c.remove_option(OP_QUERY_EXHAUST)
    assert_equal 0, c.options & OP_QUERY_EXHAUST

    c.next
    assert_raises Mongo::InvalidOperation do
      c.add_option(OP_QUERY_EXHAUST)
    end

    assert_raises Mongo::InvalidOperation do
      c.add_option(OP_QUERY_EXHAUST)
    end
  end

  def test_exhaust
    skip("Mongo Version is not >= 2.0") unless Cfg.version >= "2.0"
      
    data = "1" * 100_000
    10_000.times do |n|
      Cfg.coll.insert({:n => n, :data => data})
    end

    c = Cursor.new(Cfg.coll)
    c.add_option(OP_QUERY_EXHAUST)
    assert_equal Cfg.coll.count, c.to_a.size
    assert c.closed?

    c = Cursor.new(Cfg.coll)
    c.add_option(OP_QUERY_EXHAUST)
    9999.times do
      c.next
    end
    assert c.has_next?
    assert c.next
    assert !c.has_next?
    assert c.closed?

  end

  # def test_inspect
  #   selector = {:a => 1}
  #   cursor = Cfg.coll.find(selector)
  #   assert_equal "<Mongo::Cursor:0x#{cursor.object_id.to_s(16)} namespace='#{Cfg.db.name}.#{Cfg.coll.name}' " +
  #       "@selector=#{selector.inspect} @cursor_id=#{cursor.cursor_id}>", cursor.inspect
  # end

  def test_explain
    Cfg.coll.insert('a' => 1)
    cursor = Cfg.coll.find('a' => 1)
    explaination = cursor.explain
    assert explaination['cursor']
    assert_kind_of Numeric, explaination['n']
    assert_kind_of Numeric, explaination['millis']
    assert_kind_of Numeric, explaination['nscanned']
  end

  def test_count
    
    assert_equal 0, Cfg.coll.find().count()

    10.times do |i|
      Cfg.coll.save("x" => i)
    end

    assert_equal 10, Cfg.coll.find().count()
    assert_kind_of Integer, Cfg.coll.find().count()
    assert_equal 10, Cfg.coll.find({}, :limit => 5).count()
    assert_equal 10, Cfg.coll.find({}, :skip => 5).count()

    assert_equal 5, Cfg.coll.find({}, :limit => 5).count(true)
    assert_equal 5, Cfg.coll.find({}, :skip => 5).count(true)
    assert_equal 2, Cfg.coll.find({}, :skip => 5, :limit => 2).count(true)

    assert_equal 1, Cfg.coll.find({"x" => 1}).count()
    assert_equal 5, Cfg.coll.find({"x" => {"$lt" => 5}}).count()

    a = Cfg.coll.find()
    b = a.count()
    a.each do |doc|
      break
    end
    assert_equal b, a.count()

    assert_equal 0, Cfg.db['acollectionthatdoesn'].count()
  end

  def test_sort
    
    5.times{|x| Cfg.coll.insert({"age" => x}) }

    assert_kind_of Cursor, Cfg.coll.find().sort(:age, 1)

    assert_equal 0, Cfg.coll.find().sort(:age, 1).next_document["age"]
    assert_equal 4, Cfg.coll.find().sort(:age, -1).next_document["age"]
    assert_equal 0, Cfg.coll.find().sort([["age", :asc]]).next_document["age"]

    assert_kind_of Cursor, Cfg.coll.find().sort([[:age, -1], [:b, 1]])

    assert_equal 4, Cfg.coll.find().sort(:age, 1).sort(:age, -1).next_document["age"]
    assert_equal 0, Cfg.coll.find().sort(:age, -1).sort(:age, 1).next_document["age"]

    assert_equal 4, Cfg.coll.find().sort([:age, :asc]).sort(:age, -1).next_document["age"]
    assert_equal 0, Cfg.coll.find().sort([:age, :desc]).sort(:age, 1).next_document["age"]

    cursor = Cfg.coll.find()
    cursor.next_document
    assert_raises InvalidOperation do
      cursor.sort(["age", 1])
    end

    assert_raises InvalidSortValueError do
      Cfg.coll.find().sort(:age, 25).next_document
    end

    assert_raises InvalidSortValueError do
      Cfg.coll.find().sort(25).next_document
    end
  end

  def test_sort_date
    
    5.times{|x| Cfg.coll.insert({"created_at" => Time.utc(2000 + x)}) }

    assert_equal 2000, Cfg.coll.find().sort(:created_at, :asc).next_document["created_at"].year
    assert_equal 2004, Cfg.coll.find().sort(:created_at, :desc).next_document["created_at"].year

    assert_equal 2000, Cfg.coll.find().sort([:created_at, :asc]).next_document["created_at"].year
    assert_equal 2004, Cfg.coll.find().sort([:created_at, :desc]).next_document["created_at"].year

    assert_equal 2000, Cfg.coll.find().sort([[:created_at, :asc]]).next_document["created_at"].year
    assert_equal 2004, Cfg.coll.find().sort([[:created_at, :desc]]).next_document["created_at"].year
  end

  def test_sort_min_max_keys
    
    Cfg.coll.insert({"n" => 1000000})
    Cfg.coll.insert({"n" => -1000000})
    Cfg.coll.insert({"n" => MaxKey.new})
    Cfg.coll.insert({"n" => MinKey.new})

    results = Cfg.coll.find.sort([:n, :asc]).to_a

    assert_equal MinKey.new, results[0]['n']
    assert_equal(-1000000,   results[1]['n'])
    assert_equal 1000000,    results[2]['n']
    assert_equal MaxKey.new, results[3]['n']
  end

  def test_id_range_queries
    
    t1 = Time.now
    t1_id = ObjectId.from_time(t1)
    Cfg.coll.save({:t => 't1'})
    Cfg.coll.save({:t => 't1'})
    Cfg.coll.save({:t => 't1'})
    sleep(2)
    t2 = Time.now
    t2_id = ObjectId.from_time(t2)
    Cfg.coll.save({:t => 't2'})
    Cfg.coll.save({:t => 't2'})
    Cfg.coll.save({:t => 't2'})

    assert_equal 3, Cfg.coll.find({'_id' => {'$gt' => t1_id, '$lt' => t2_id}}).count
    Cfg.coll.find({'_id' => {'$gt' => t2_id}}).each do |doc|
      assert_equal 't2', doc['t']
    end
  end

  def test_limit
    
    10.times do |i|
      Cfg.coll.save("x" => i)
    end
    assert_equal 10, Cfg.coll.find().count()

    results = Cfg.coll.find().limit(5).to_a
    assert_equal 5, results.length
  end

  def test_timeout_options
    cursor = Cursor.new(Cfg.coll)
    assert_equal true, cursor.timeout

    cursor = Cfg.coll.find
    assert_equal true, cursor.timeout

    cursor = Cfg.coll.find({}, :timeout => nil)
    assert_equal true, cursor.timeout

    cursor = Cursor.new(Cfg.coll, :timeout => false)
    assert_equal false, cursor.timeout

    Cfg.coll.find({}, :timeout => false) do |c|
      assert_equal false, c.timeout
    end
  end

  def test_timeout
    opts = Cursor.new(Cfg.coll).query_opts
    assert_equal 0, opts & Mongo::OP_QUERY_NO_CURSOR_TIMEOUT

    opts = Cursor.new(Cfg.coll, :timeout => false).query_opts
    assert_equal Mongo::OP_QUERY_NO_CURSOR_TIMEOUT,
      opts & Mongo::OP_QUERY_NO_CURSOR_TIMEOUT
  end

  def test_limit_exceptions
    cursor      = Cfg.coll.find()
    firstResult = cursor.next_document
    assert_raises InvalidOperation, "Cannot modify the query once it has been run or closed." do
      cursor.limit(1)
    end

    cursor = Cfg.coll.find()
    cursor.close
    assert_raises InvalidOperation, "Cannot modify the query once it has been run or closed." do
      cursor.limit(1)
    end
  end

  def test_skip
    

    10.times do |i|
      Cfg.coll.save("x" => i)
    end
    assert_equal 10, Cfg.coll.find().count()

    all_results    = Cfg.coll.find().to_a
    skip_results = Cfg.coll.find().skip(2).to_a
    assert_equal 10, all_results.length
    assert_equal 8,  skip_results.length

    assert_equal all_results.slice(2...10), skip_results
  end

  def test_skip_exceptions
    cursor      = Cfg.coll.find()
    firstResult = cursor.next_document
    assert_raises InvalidOperation, "Cannot modify the query once it has been run or closed." do
      cursor.skip(1)
    end

    cursor = Cfg.coll.find()
    cursor.close
    assert_raises InvalidOperation, "Cannot modify the query once it has been run or closed." do
      cursor.skip(1)
    end
  end

  def test_limit_skip_chaining
    
    10.times do |i|
      Cfg.coll.save("x" => i)
    end

    all_results = Cfg.coll.find().to_a
    limited_skip_results = Cfg.coll.find().limit(5).skip(3).to_a

    assert_equal all_results.slice(3...8), limited_skip_results
  end

  def test_close_no_query_sent
    begin
      cursor = Cfg.coll.find('a' => 1)
      cursor.close
      assert cursor.closed?
    rescue => ex
      fail ex.to_s
    end
  end

  def test_refill_via_get_more
    Cfg.coll.insert('a' => 1)
    assert_equal 1, Cfg.coll.count
    1000.times { |i|
      assert_equal 1 + i, Cfg.coll.count
      Cfg.coll.insert('a' => i)
    }

    assert_equal 1001, Cfg.coll.count
    count = 0
    Cfg.coll.find.each { |obj|
      count += obj['a']
    }
    assert_equal 1001, Cfg.coll.count

    # do the same thing again for debugging
    assert_equal 1001, Cfg.coll.count
    count2 = 0
    Cfg.coll.find.each { |obj|
      count2 += obj['a']
    }
    assert_equal 1001, Cfg.coll.count

    assert_equal count, count2
    assert_equal 499501, count
  end

  def test_refill_via_get_more_alt_coll
    coll = Cfg.db.collection('test-alt-coll')
    coll.remove
    coll.insert('a' => 1)     # collection not created until it's used
    assert_equal 1, coll.count

    1000.times { |i|
      assert_equal 1 + i, coll.count
      coll.insert('a' => i)
    }

    assert_equal 1001, coll.count
    count = 0
    coll.find.each { |obj|
      count += obj['a']
    }
    assert_equal 1001, coll.count

    # do the same thing again for debugging
    assert_equal 1001, coll.count
    count2 = 0
    coll.find.each { |obj|
      count2 += obj['a']
    }
    assert_equal 1001, coll.count

    assert_equal count, count2
    assert_equal 499501, count
  end

  def test_close_after_query_sent
    begin
      cursor = Cfg.coll.find('a' => 1)
      cursor.next_document
      cursor.close
      assert cursor.closed?
    rescue => ex
      fail ex.to_s
    end
  end

  def test_kill_cursors
    Cfg.coll.drop

    client_cursors = Cfg.db.command("cursorInfo" => 1)["clientCursors_size"]

    10000.times do |i|
      Cfg.coll.insert("i" => i)
    end

    assert_equal(client_cursors,
                 Cfg.db.command("cursorInfo" => 1)["clientCursors_size"])

    10.times do |i|
      Cfg.coll.find_one()
    end

    assert_equal(client_cursors,
                 Cfg.db.command("cursorInfo" => 1)["clientCursors_size"])

    10.times do |i|
      a = Cfg.coll.find()
      a.next_document
      a.close()
    end

    assert_equal(client_cursors,
                 Cfg.db.command("cursorInfo" => 1)["clientCursors_size"])

    a = Cfg.coll.find()
    a.next_document

    refute_equal(client_cursors,
                     Cfg.db.command("cursorInfo" => 1)["clientCursors_size"])

    a.close()

    assert_equal(client_cursors,
                 Cfg.db.command("cursorInfo" => 1)["clientCursors_size"])

    a = Cfg.coll.find({}, :limit => 10).next_document

    assert_equal(client_cursors,
                 Cfg.db.command("cursorInfo" => 1)["clientCursors_size"])

    Cfg.coll.find() do |cursor|
      cursor.next_document
    end

    assert_equal(client_cursors,
                 Cfg.db.command("cursorInfo" => 1)["clientCursors_size"])

    Cfg.coll.find() { |cursor|
      cursor.next_document
    }

    assert_equal(client_cursors,
                 Cfg.db.command("cursorInfo" => 1)["clientCursors_size"])
  end

  def test_count_with_fields
    Cfg.coll.save("x" => 1)

    assert_equal(1, Cfg.coll.find({}, :fields => ["a"]).count())
  end

  def test_has_next
    
    200.times do |n|
      Cfg.coll.save("x" => n)
    end

    cursor = Cfg.coll.find
    n = 0
    while cursor.has_next?
      assert cursor.next
      n += 1
    end

    assert_equal n, 200
    assert_equal false, cursor.has_next?
  end

  def test_cursor_invalid
    
    10000.times do |n|
      Cfg.coll.insert({:a => n})
    end

    cursor = Cfg.coll.find({})

    # assert_raises_error Mongo::OperationFailure, "CURSOR_NOT_FOUND" do
    #   9999.times do
    #     cursor.next_document
    #     cursor.instance_variable_set(:@cursor_id, 1234567890)
    #   end
    # end
  end

  def test_enumberables
    
    100.times do |n|
      Cfg.coll.insert({:a => n})
    end

    assert_equal 100, Cfg.coll.find.to_a.length
    assert_equal 100, Cfg.coll.find.to_set.length

    cursor = Cfg.coll.find
    50.times { |n| cursor.next_document }
    assert_equal 50, cursor.to_a.length
  end

  def test_rewind
    
    100.times do |n|
      Cfg.coll.insert({:a => n})
    end

    cursor = Cfg.coll.find
    cursor.to_a
    assert_equal false, cursor.has_next?

    cursor.rewind!
    assert_equal 100, cursor.map {|doc| doc }.length

    cursor.rewind!
    5.times { cursor.next_document }
    cursor.rewind!
    assert_equal 100, cursor.map {|doc| doc }.length
  end

  def test_transformer
    transformer = Proc.new { |doc| doc }
    cursor = Cursor.new(Cfg.coll, :transformer => transformer)
    assert_equal(transformer, cursor.transformer)
  end

  def test_instance_transformation_with_next
    Cfg.coll.insert('a' => 1)
    klass       = Struct.new(:id, :a)
    transformer = Proc.new { |doc| klass.new(doc['_id'], doc['a']) }
    cursor      = Cursor.new(Cfg.coll, :transformer => transformer)
    instance    = cursor.next

    assert_instance_of(klass, instance)
    assert_instance_of(BSON::ObjectId, instance.id)
    assert_equal(1, instance.a)
  end

  def test_instance_transformation_with_each
    klass       = Struct.new(:id, :a)
    transformer = Proc.new { |doc| klass.new(doc['_id'], doc['a']) }
    cursor      = Cursor.new(Cfg.coll, :transformer => transformer)

    cursor.each do |instance|
      assert_instance_of(klass, instance)
    end
  end
end
