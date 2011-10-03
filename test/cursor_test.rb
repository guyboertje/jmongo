require './test/test_helper'
require 'logger'

CONNECTION ||= Mongo::Connection.new(TEST_HOST, TEST_PORT, :op_timeout => 10)
$db   = CONNECTION.db(MONGO_TEST_DB)

VERSION = CONNECTION.server_version
apr VERSION

def clear_collections
  $db.collection_names.each do |n|
    $db.drop_collection(n) unless n =~ /system/
  end
end

clear_collections

$coll = $db.collection("test")
$coll_full_name = "#{MONGO_TEST_DB}.test"

class CursorTest < MiniTest::Unit::TestCase
  include Mongo

  def setup
    clear_collections
    #$coll.insert('a' => 1)     # collection not created until it's used
  end

  def test_alive
    batch = []
    5000.times do |n|
      batch << {:a => n}
    end

    $coll.insert(batch)
    cursor = $coll.find
    assert !cursor.alive?
    cursor.next
    assert cursor.alive?
    cursor.close
    assert !cursor.alive?
  end

  def test_add_and_remove_options
    $coll.insert('a' => 1)
    c = $coll.find
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
    skip("Mongo Version is not >= 2.0") unless VERSION >= "2.0"
      
    data = "1" * 100_000
    10_000.times do |n|
      $coll.insert({:n => n, :data => data})
    end

    c = Cursor.new($coll)
    c.add_option(OP_QUERY_EXHAUST)
    assert_equal $coll.count, c.to_a.size
    assert c.closed?

    c = Cursor.new($coll)
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
  #   cursor = $coll.find(selector)
  #   assert_equal "<Mongo::Cursor:0x#{cursor.object_id.to_s(16)} namespace='#{$db.name}.#{$coll.name}' " +
  #       "@selector=#{selector.inspect} @cursor_id=#{cursor.cursor_id}>", cursor.inspect
  # end

  def test_explain
    $coll.insert('a' => 1)
    cursor = $coll.find('a' => 1)
    explaination = cursor.explain
    assert explaination['cursor']
    assert_kind_of Numeric, explaination['n']
    assert_kind_of Numeric, explaination['millis']
    assert_kind_of Numeric, explaination['nscanned']
  end

  def test_count
    
    assert_equal 0, $coll.find().count()

    10.times do |i|
      $coll.save("x" => i)
    end

    assert_equal 10, $coll.find().count()
    assert_kind_of Integer, $coll.find().count()
    assert_equal 10, $coll.find({}, :limit => 5).count()
    assert_equal 10, $coll.find({}, :skip => 5).count()

    assert_equal 5, $coll.find({}, :limit => 5).count(true)
    assert_equal 5, $coll.find({}, :skip => 5).count(true)
    assert_equal 2, $coll.find({}, :skip => 5, :limit => 2).count(true)

    assert_equal 1, $coll.find({"x" => 1}).count()
    assert_equal 5, $coll.find({"x" => {"$lt" => 5}}).count()

    a = $coll.find()
    b = a.count()
    a.each do |doc|
      break
    end
    assert_equal b, a.count()

    assert_equal 0, $db['acollectionthatdoesn'].count()
  end

  def test_sort
    
    5.times{|x| $coll.insert({"age" => x}) }

    assert_kind_of Cursor, $coll.find().sort(:age, 1)

    assert_equal 0, $coll.find().sort(:age, 1).next_document["age"]
    assert_equal 4, $coll.find().sort(:age, -1).next_document["age"]
    assert_equal 0, $coll.find().sort([["age", :asc]]).next_document["age"]

    assert_kind_of Cursor, $coll.find().sort([[:age, -1], [:b, 1]])

    assert_equal 4, $coll.find().sort(:age, 1).sort(:age, -1).next_document["age"]
    assert_equal 0, $coll.find().sort(:age, -1).sort(:age, 1).next_document["age"]

    assert_equal 4, $coll.find().sort([:age, :asc]).sort(:age, -1).next_document["age"]
    assert_equal 0, $coll.find().sort([:age, :desc]).sort(:age, 1).next_document["age"]

    cursor = $coll.find()
    cursor.next_document
    assert_raises InvalidOperation do
      cursor.sort(["age", 1])
    end

    assert_raises InvalidSortValueError do
      $coll.find().sort(:age, 25).next_document
    end

    assert_raises InvalidSortValueError do
      $coll.find().sort(25).next_document
    end
  end

  def test_sort_date
    
    5.times{|x| $coll.insert({"created_at" => Time.utc(2000 + x)}) }

    assert_equal 2000, $coll.find().sort(:created_at, :asc).next_document["created_at"].year
    assert_equal 2004, $coll.find().sort(:created_at, :desc).next_document["created_at"].year

    assert_equal 2000, $coll.find().sort([:created_at, :asc]).next_document["created_at"].year
    assert_equal 2004, $coll.find().sort([:created_at, :desc]).next_document["created_at"].year

    assert_equal 2000, $coll.find().sort([[:created_at, :asc]]).next_document["created_at"].year
    assert_equal 2004, $coll.find().sort([[:created_at, :desc]]).next_document["created_at"].year
  end

  def test_sort_min_max_keys
    
    $coll.insert({"n" => 1000000})
    $coll.insert({"n" => -1000000})
    $coll.insert({"n" => MaxKey.new})
    $coll.insert({"n" => MinKey.new})

    results = $coll.find.sort([:n, :asc]).to_a

    assert_equal MinKey.new, results[0]['n']
    assert_equal(-1000000,   results[1]['n'])
    assert_equal 1000000,    results[2]['n']
    assert_equal MaxKey.new, results[3]['n']
  end

  def test_id_range_queries
    
    t1 = Time.now
    t1_id = ObjectId.from_time(t1)
    $coll.save({:t => 't1'})
    $coll.save({:t => 't1'})
    $coll.save({:t => 't1'})
    sleep(2)
    t2 = Time.now
    t2_id = ObjectId.from_time(t2)
    $coll.save({:t => 't2'})
    $coll.save({:t => 't2'})
    $coll.save({:t => 't2'})

    assert_equal 3, $coll.find({'_id' => {'$gt' => t1_id, '$lt' => t2_id}}).count
    $coll.find({'_id' => {'$gt' => t2_id}}).each do |doc|
      assert_equal 't2', doc['t']
    end
  end

  def test_limit
    
    10.times do |i|
      $coll.save("x" => i)
    end
    assert_equal 10, $coll.find().count()

    results = $coll.find().limit(5).to_a
    assert_equal 5, results.length
  end

  def test_timeout_options
    cursor = Cursor.new($coll)
    assert_equal true, cursor.timeout

    cursor = $coll.find
    assert_equal true, cursor.timeout

    cursor = $coll.find({}, :timeout => nil)
    assert_equal true, cursor.timeout

    cursor = Cursor.new($coll, :timeout => false)
    assert_equal false, cursor.timeout

    $coll.find({}, :timeout => false) do |c|
      assert_equal false, c.timeout
    end
  end

  def test_timeout
    opts = Cursor.new($coll).query_opts
    assert_equal 0, opts & Mongo::OP_QUERY_NO_CURSOR_TIMEOUT

    opts = Cursor.new($coll, :timeout => false).query_opts
    assert_equal Mongo::OP_QUERY_NO_CURSOR_TIMEOUT,
      opts & Mongo::OP_QUERY_NO_CURSOR_TIMEOUT
  end

  def test_limit_exceptions
    cursor      = $coll.find()
    firstResult = cursor.next_document
    assert_raises InvalidOperation, "Cannot modify the query once it has been run or closed." do
      cursor.limit(1)
    end

    cursor = $coll.find()
    cursor.close
    assert_raises InvalidOperation, "Cannot modify the query once it has been run or closed." do
      cursor.limit(1)
    end
  end

  def test_skip
    

    10.times do |i|
      $coll.save("x" => i)
    end
    assert_equal 10, $coll.find().count()

    all_results    = $coll.find().to_a
    skip_results = $coll.find().skip(2).to_a
    assert_equal 10, all_results.length
    assert_equal 8,  skip_results.length

    assert_equal all_results.slice(2...10), skip_results
  end

  def test_skip_exceptions
    cursor      = $coll.find()
    firstResult = cursor.next_document
    assert_raises InvalidOperation, "Cannot modify the query once it has been run or closed." do
      cursor.skip(1)
    end

    cursor = $coll.find()
    cursor.close
    assert_raises InvalidOperation, "Cannot modify the query once it has been run or closed." do
      cursor.skip(1)
    end
  end

  def test_limit_skip_chaining
    
    10.times do |i|
      $coll.save("x" => i)
    end

    all_results = $coll.find().to_a
    limited_skip_results = $coll.find().limit(5).skip(3).to_a

    assert_equal all_results.slice(3...8), limited_skip_results
  end

  def test_close_no_query_sent
    begin
      cursor = $coll.find('a' => 1)
      cursor.close
      assert cursor.closed?
    rescue => ex
      fail ex.to_s
    end
  end

  def test_refill_via_get_more
    $coll.insert('a' => 1)
    assert_equal 1, $coll.count
    1000.times { |i|
      assert_equal 1 + i, $coll.count
      $coll.insert('a' => i)
    }

    assert_equal 1001, $coll.count
    count = 0
    $coll.find.each { |obj|
      count += obj['a']
    }
    assert_equal 1001, $coll.count

    # do the same thing again for debugging
    assert_equal 1001, $coll.count
    count2 = 0
    $coll.find.each { |obj|
      count2 += obj['a']
    }
    assert_equal 1001, $coll.count

    assert_equal count, count2
    assert_equal 499501, count
  end

  def test_refill_via_get_more_alt_coll
    coll = $db.collection('test-alt-coll')
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
      cursor = $coll.find('a' => 1)
      cursor.next_document
      cursor.close
      assert cursor.closed?
    rescue => ex
      fail ex.to_s
    end
  end

  def test_kill_cursors
    $coll.drop

    client_cursors = $db.command("cursorInfo" => 1)["clientCursors_size"]

    10000.times do |i|
      $coll.insert("i" => i)
    end

    assert_equal(client_cursors,
                 $db.command("cursorInfo" => 1)["clientCursors_size"])

    10.times do |i|
      $coll.find_one()
    end

    assert_equal(client_cursors,
                 $db.command("cursorInfo" => 1)["clientCursors_size"])

    10.times do |i|
      a = $coll.find()
      a.next_document
      a.close()
    end

    assert_equal(client_cursors,
                 $db.command("cursorInfo" => 1)["clientCursors_size"])

    a = $coll.find()
    a.next_document

    refute_equal(client_cursors,
                     $db.command("cursorInfo" => 1)["clientCursors_size"])

    a.close()

    assert_equal(client_cursors,
                 $db.command("cursorInfo" => 1)["clientCursors_size"])

    a = $coll.find({}, :limit => 10).next_document

    assert_equal(client_cursors,
                 $db.command("cursorInfo" => 1)["clientCursors_size"])

    $coll.find() do |cursor|
      cursor.next_document
    end

    assert_equal(client_cursors,
                 $db.command("cursorInfo" => 1)["clientCursors_size"])

    $coll.find() { |cursor|
      cursor.next_document
    }

    assert_equal(client_cursors,
                 $db.command("cursorInfo" => 1)["clientCursors_size"])
  end

  def test_count_with_fields
    $coll.save("x" => 1)

    assert_equal(1, $coll.find({}, :fields => ["a"]).count())
  end

  def test_has_next
    
    200.times do |n|
      $coll.save("x" => n)
    end

    cursor = $coll.find
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
      $coll.insert({:a => n})
    end

    cursor = $coll.find({})

    # assert_raises_error Mongo::OperationFailure, "CURSOR_NOT_FOUND" do
    #   9999.times do
    #     cursor.next_document
    #     cursor.instance_variable_set(:@cursor_id, 1234567890)
    #   end
    # end
  end

  def test_enumberables
    
    100.times do |n|
      $coll.insert({:a => n})
    end

    assert_equal 100, $coll.find.to_a.length
    assert_equal 100, $coll.find.to_set.length

    cursor = $coll.find
    50.times { |n| cursor.next_document }
    assert_equal 50, cursor.to_a.length
  end

  def test_rewind
    
    100.times do |n|
      $coll.insert({:a => n})
    end

    cursor = $coll.find
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
    cursor = Cursor.new($coll, :transformer => transformer)
    assert_equal(transformer, cursor.transformer)
  end

  def test_instance_transformation_with_next
    $coll.insert('a' => 1)
    klass       = Struct.new(:id, :a)
    transformer = Proc.new { |doc| klass.new(doc['_id'], doc['a']) }
    cursor      = Cursor.new($coll, :transformer => transformer)
    instance    = cursor.next

    assert_instance_of(klass, instance)
    assert_instance_of(BSON::ObjectId, instance.id)
    assert_equal(1, instance.a)
  end

  def test_instance_transformation_with_each
    klass       = Struct.new(:id, :a)
    transformer = Proc.new { |doc| klass.new(doc['_id'], doc['a']) }
    cursor      = Cursor.new($coll, :transformer => transformer)

    cursor.each do |instance|
      assert_instance_of(klass, instance)
    end
  end
end
