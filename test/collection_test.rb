require './test/test_helper'

Cfg.connection :op_timeout => 10
Cfg.db

class TestCollection < MiniTest::Unit::TestCase

  def setup
    Cfg.clear_all
  end

  def test_capped_method

    Cfg.db.create_collection('normal').insert('x'=>3)
    assert !Cfg.db['normal'].capped?

    Cfg.db.create_collection('c', :capped => true, :size => 100_000).insert('g'=>4)
    assert Cfg.db['c'].capped?
  end

  def test_optional_pk_factory
    @coll_default_pk = Cfg.db.collection('stuff')
    assert_equal BSON::ObjectId, @coll_default_pk.pk_factory
    @coll_default_pk = Cfg.db.create_collection('more-stuff')
    assert_equal BSON::ObjectId, @coll_default_pk.pk_factory

    # Create a db with a pk_factory.
    @db = Connection.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
                         ENV['MONGO_RUBY_DRIVER_PORT'] || Connection::DEFAULT_PORT).db(MONGO_TEST_DB, :pk => Object.new)
    @coll = @db.collection('coll-with-pk')
    assert @coll.pk_factory.is_a?(Object)

    @coll = @db.create_collection('created_coll_with_pk')
    assert @coll.pk_factory.is_a?(Object)
  end

  class TestPK
    def self.create_pk
    end
  end

  def test_pk_factory_on_collection
    @coll2 = Collection.new('foo', Cfg.db, :pk => TestPK)
    assert_equal TestPK, @coll2.pk_factory
  end

  def test_valid_names
    assert_raises Mongo::InvalidNSName do
      Cfg.db["te$t"]
    end

    assert_raises Mongo::InvalidNSName do
      Cfg.db['$main']
    end

    assert Cfg.db['$cmd']
    assert Cfg.db['oplog.$main']
  end

  def test_collection
    assert_kind_of Collection, Cfg.db["test"]
    assert_equal Cfg.db["test"].name(), Cfg.db.collection("test").name()
    assert_equal Cfg.db["test"].name(), Cfg.db[:test].name()

    assert_kind_of Collection, Cfg.db["test"]["foo"]
    assert_equal Cfg.db["test"]["foo"].name(), Cfg.db.collection("test.foo").name()
    assert_equal Cfg.db["test"]["foo"].name(), Cfg.db["test.foo"].name()

    Cfg.db["test"]["foo"].remove
    Cfg.db["test"]["foo"].insert("x" => 5)
    assert_equal 5, Cfg.db.collection("test.foo").find_one()["x"]
  end

  def test_rename_collection
    @col = Cfg.db.create_collection('foo1')
    @col.insert("x" => 5) #must insert something to actually create collection
    assert_equal 'foo1', @col.name
    @col.rename('bar1')
    assert_equal 'bar1', @col.name
  end

  def test_nil_id
    skip("The Java driver does not allow nil _id")
    assert_equal 5, Cfg.test.insert({"_id" => 5, "foo" => "bar"}, {:safe => true})
    assert_equal 5, Cfg.test.save({"_id" => 5, "foo" => "baz"}, {:safe => true})
    assert_equal nil, Cfg.test.find_one("foo" => "bar")
    assert_equal "baz", Cfg.test.find_one(:_id => 5)["foo"]
    assert_raises OperationFailure do
      Cfg.test.insert({"_id" => 5, "foo" => "bar"}, {:safe => true})
    end

    assert_equal nil, Cfg.test.insert({"_id" => nil, "foo" => "bar"}, {:safe => true})
    assert_equal nil, Cfg.test.save({"_id" => nil, "foo" => "baz"}, {:safe => true})
    assert_equal nil, Cfg.test.find_one("foo" => "bar")
    assert_equal "baz", Cfg.test.find_one(:_id => nil)["foo"]
    assert_raises OperationFailure do
      Cfg.test.insert({"_id" => nil, "foo" => "bar"}, {:safe => true})
    end
    assert_raises OperationFailure do
      Cfg.test.insert({:_id => nil, "foo" => "bar"}, {:safe => true})
    end
  end

  if Cfg.version > "1.1"
    def setup_for_distinct
      Cfg.test.remove
      Cfg.test.insert([{:a => 0, :b => {:c => "a"}},
                     {:a => 1, :b => {:c => "b"}},
                     {:a => 1, :b => {:c => "c"}},
                     {:a => 2, :b => {:c => "a"}},
                     {:a => 3},
                     {:a => 3}])
    end

    def test_distinct_queries
      setup_for_distinct
      assert_equal [0, 1, 2, 3], Cfg.test.distinct(:a).sort
      assert_equal ["a", "b", "c"], Cfg.test.distinct("b.c").sort
    end

    if Cfg.version >= "1.2"
      def test_filter_collection_with_query
        setup_for_distinct
        assert_equal [2, 3], Cfg.test.distinct(:a, {:a => {"$gt" => 1}}).sort
      end

      def test_filter_nested_objects
        setup_for_distinct
        assert_equal ["a", "b"], Cfg.test.distinct("b.c", {"b.c" => {"$ne" => "c"}}).sort
      end
    end
  end

  def test_safe_insert
    Cfg.test.create_index("hello", :unique => true)
    a = {"hello" => "world"}
    Cfg.test.insert(a)
    Cfg.test.insert(a)
    assert(Cfg.db.get_last_error['err'].include?("11000"))

    assert_raises OperationFailure do
      Cfg.test.insert(a, :safe => true)
    end
  end

  def test_bulk_insert_with_continue_on_error
    if Cfg.version >= "2.0"
      Cfg.test.create_index([["foo", 1]], :unique => true)
      docs = []
      docs << {:foo => 1}
      docs << {:foo => 1}
      docs << {:foo => 2}
      docs << {:foo => 3}
      assert_raises OperationFailure do
        Cfg.test.insert(docs, :safe => true)
      end
      assert_equal 1, Cfg.test.count
      Cfg.test.remove

      docs = []
      docs << {:foo => 1}
      docs << {:foo => 1}
      docs << {:foo => 2}
      docs << {:foo => 3}
      docs << {:foo => 3}
      assert_raises OperationFailure do
        Cfg.test.insert(docs, :safe => true, :continue_on_error => true)
      end
      assert_equal 3, Cfg.test.count

      Cfg.test.remove
      Cfg.test.drop_index("foo_1")
    end
  end

  def test_maximum_insert_size
    skip("The Java driver does not enforce a maximum")
    docs = []
    16.times do
      docs << {'foo' => 'a' * 1_000_000}
    end

    assert_raises InvalidOperation do
      Cfg.test.insert(docs)
    end
  end

  def test_update
    id1 = Cfg.test.save("x" => 5)
    Cfg.test.update({}, {"$inc" => {"x" => 1}})
    assert_equal 1, Cfg.test.count()
    assert_equal 6, Cfg.test.find_one(:_id => id1)["x"]

    id2 = Cfg.test.save("x" => 1)
    Cfg.test.update({"x" => 6}, {"$inc" => {"x" => 1}})
    assert_equal 7, Cfg.test.find_one(:_id => id1)["x"]
    assert_equal 1, Cfg.test.find_one(:_id => id2)["x"]
  end

  def test_multi_update
    Cfg.test.save("num" => 10)
    Cfg.test.save("num" => 10)
    Cfg.test.save("num" => 10)
    assert_equal 3, Cfg.test.count

    Cfg.test.update({"num" => 10}, {"$set" => {"num" => 100}}, :multi => true)
    Cfg.test.find.each do |doc|
      assert_equal 100, doc["num"]
    end
  end

  def test_upsert
    Cfg.test.update({"page" => "/"}, {"$inc" => {"count" => 1}}, :upsert => true)
    Cfg.test.update({"page" => "/"}, {"$inc" => {"count" => 1}}, :upsert => true)

    assert_equal 1, Cfg.test.count()
    assert_equal 2, Cfg.test.find_one()["count"]
  end

  def test_safe_update
    Cfg.test.create_index("x", :unique => true)
    Cfg.test.insert("x" => 5)
    Cfg.test.insert("x" => 10)

    # Can update an indexed collection.
    Cfg.test.update({}, {"$inc" => {"x" => 1}})
    assert !Cfg.db.error?

    # Can't duplicate an index.
    assert_raises OperationFailure do
      Cfg.test.update({}, {"x" => 10}, :safe => true)
    end
  end

  def test_safe_save
    Cfg.test.create_index("hello", :unique => true)

    Cfg.test.save("hello" => "world")
    Cfg.test.save("hello" => "world")

    assert_raises OperationFailure do
      Cfg.test.save({"hello" => "world"}, :safe => true)
    end
  end

  def test_safe_remove
    @conn = Cfg.new_connection
    @db   = @conn[MONGO_TEST_DB]
    @test = @db['test-safe-remove']
    @test.save({:a => 50})
    assert_equal 1, @test.remove({}, :safe => true)["n"]
    @test.drop
  end

  def test_remove_return_value
    assert_equal true, Cfg.test.remove({})
  end

  def test_count

    assert_equal 0, Cfg.test.count
    Cfg.test.save(:x => 1)
    Cfg.test.save(:x => 2)
    assert_equal 2, Cfg.test.count

    assert_equal 1, Cfg.test.count(:query => {:x => 1})
    assert_equal 1, Cfg.test.count(:limit => 1)
    assert_equal 0, Cfg.test.count(:skip => 2)
  end

  # Note: #size is just an alias for #count.
  def test_size
    assert_equal 0, Cfg.test.count
    assert_equal Cfg.test.size, Cfg.test.count
    Cfg.test.save("x" => 1)
    Cfg.test.save("x" => 2)
    assert_equal Cfg.test.size, Cfg.test.count
  end

  def test_no_timeout_option

    assert_raises ArgumentError, "Timeout can be set to false only when #find is invoked with a block." do
      Cfg.test.find({}, :timeout => false)
    end

    Cfg.test.find({}, :timeout => false) do |cursor|
      assert_equal 0, cursor.count
    end

    Cfg.test.save("x" => 1)
    Cfg.test.save("x" => 2)
    Cfg.test.find({}, :timeout => false) do |cursor|
      assert_equal 2, cursor.count
    end
  end

  def test_default_timeout
    cursor = Cfg.test.find
    assert_equal true, cursor.timeout
  end

  def test_fields_as_hash
    Cfg.test.save(:a => 1, :b => 1, :c => 1)

    doc = Cfg.test.find_one({:a => 1}, :fields => {:b => 0})
    assert_nil doc['b']
    assert doc['a']
    assert doc['c']

    doc = Cfg.test.find_one({:a => 1}, :fields => {:a => 1, :b => 1})
    assert_nil doc['c']
    assert doc['a']
    assert doc['b']


    assert_raises Mongo::OperationFailure do
      Cfg.test.find_one({:a => 1}, :fields => {:a => 1, :b => 0})
    end
  end

  if Cfg.version >= "1.5.1"
    def test_fields_with_slice
      Cfg.test.save({:foo => [1, 2, 3, 4, 5, 6], :test => 'slice'})

      doc = Cfg.test.find_one({:test => 'slice'}, :fields => {'foo' => {'$slice' => [0, 3]}})
      assert_equal [1, 2, 3], doc['foo']
      Cfg.test.remove
    end
  end

  def test_find_one
    id = Cfg.test.save("hello" => "world", "foo" => "bar")

    assert_equal "world", Cfg.test.find_one()["hello"]
    assert_equal Cfg.test.find_one(id), Cfg.test.find_one()
    assert_equal Cfg.test.find_one(nil), Cfg.test.find_one()
    assert_equal Cfg.test.find_one({}), Cfg.test.find_one()
    assert_equal Cfg.test.find_one("hello" => "world"), Cfg.test.find_one()
    assert_equal Cfg.test.find_one(BSON::OrderedHash["hello", "world"]), Cfg.test.find_one()

    assert Cfg.test.find_one(nil, :fields => ["hello"]).include?("hello")
    assert !Cfg.test.find_one(nil, :fields => ["foo"]).include?("hello")
    assert_equal ["_id"], Cfg.test.find_one(nil, :fields => []).keys()

    assert_equal nil, Cfg.test.find_one("hello" => "foo")
    assert_equal nil, Cfg.test.find_one(BSON::OrderedHash["hello", "foo"])
    assert_equal nil, Cfg.test.find_one(ObjectId.new)

    assert_raises TypeError do
      Cfg.test.find_one(6)
    end
  end

  def test_insert_adds_id
    doc = {"hello" => "world"}
    Cfg.test.insert(doc)
    assert(doc.include?(:_id) || doc.include?('_id'))

    docs = [{"hello" => "world"}, {"hello" => "world"}]
    Cfg.test.insert(docs)
    docs.each do |d|
      assert(d.include?(:_id) || doc.include?('_id'))
    end
  end

  def test_save_adds_id
    doc = {"hello" => "world"}
    Cfg.test.save(doc)
    assert(doc.include?(:_id) || doc.include?('_id'))
  end

  def test_optional_find_block
    10.times do |i|
      Cfg.test.save("i" => i)
    end

    x = nil
    Cfg.test.find("i" => 2) { |cursor|
      x = cursor.count()
    }
    assert_equal 1, x

    i = 0
    Cfg.test.find({}, :skip => 5) do |cursor|
      cursor.each do |doc|
        i = i + 1
      end
    end
    assert_equal 5, i

    c = nil
    Cfg.test.find() do |cursor|
      c = cursor
    end
    assert c.closed?
  end

  def test_map_reduce
    Cfg.test << { "user_id" => 1 }
    Cfg.test << { "user_id" => 2 }
    m = "function() { emit(this.user_id, 1); }"
    r = "function(k,vals) { return 1; }"
    res = Cfg.test.map_reduce(m, r, :out => 'foo');
    assert res.find_one({"_id" => 1})
    assert res.find_one({"_id" => 2})
  end

  def test_map_reduce_with_code_objects
    Cfg.test << { "user_id" => 1 }
    Cfg.test << { "user_id" => 2 }
    m = Code.new("function() { emit(this.user_id, 1); }")
    r = Code.new("function(k,vals) { return 1; }")
    res = Cfg.test.map_reduce(m, r, :out => 'foo');
    assert res.find_one({"_id" => 1})
    assert res.find_one({"_id" => 2})
  end

  def test_map_reduce_with_options
    Cfg.test << { "user_id" => 1 }
    Cfg.test << { "user_id" => 2 }
    Cfg.test << { "user_id" => 3 }
    m = Code.new("function() { emit(this.user_id, 1); }")
    r = Code.new("function(k,vals) { return 1; }")
    res = Cfg.test.map_reduce(m, r, :query => {"user_id" => {"$gt" => 1}}, :out => 'foo');
    assert_equal 2, res.count
    assert res.find_one({"_id" => 2})
    assert res.find_one({"_id" => 3})
  end

  def test_map_reduce_with_raw_response
    Cfg.test << { "user_id" => 1 }
    Cfg.test << { "user_id" => 2 }
    Cfg.test << { "user_id" => 3 }
    m = Code.new("function() { emit(this.user_id, 1); }")
    r = Code.new("function(k,vals) { return 1; }")
    res = Cfg.test.map_reduce(m, r, :raw => true, :out => 'foo')
    assert res["result"]
    assert res["counts"]
    assert res["timeMillis"]
  end

  def test_map_reduce_with_output_collection
    Cfg.test << { "user_id" => 1 }
    Cfg.test << { "user_id" => 2 }
    Cfg.test << { "user_id" => 3 }
    output_collection = "test-map-coll"
    m = Code.new("function() { emit(this.user_id, 1); }")
    r = Code.new("function(k,vals) { return 1; }")
    res = Cfg.test.map_reduce(m, r, :raw => true, :out => output_collection)
    assert_equal output_collection, res["result"]
    assert res["counts"]
    assert res["timeMillis"]
  end

  if Cfg.version >= "1.8.0"
    def test_map_reduce_with_collection_merge
      Cfg.test << {:user_id => 1}
      Cfg.test << {:user_id => 2}
      output_collection = "test-map-coll"
      m = Code.new("function() { emit(this.user_id, {count: 1}); }")
      r = Code.new("function(k,vals) { var sum = 0;" +
        " vals.forEach(function(v) { sum += v.count;} ); return {count: sum}; }")
      res = Cfg.test.map_reduce(m, r, :out => output_collection)

      Cfg.test.remove
      Cfg.test << {:user_id => 3}
      res = Cfg.test.map_reduce(m, r, :out => {:merge => output_collection})
      assert res.find.to_a.any? {|doc| doc["_id"] == 3 && doc["value"]["count"] == 1}

      Cfg.test.remove
      Cfg.test << {:user_id => 3}
      res = Cfg.test.map_reduce(m, r, :out => {:reduce => output_collection})
      assert res.find.to_a.any? {|doc| doc["_id"] == 3 && doc["value"]["count"] == 2}

      assert_raises ArgumentError do
        Cfg.test.map_reduce(m, r, :out => {:inline => 1})
      end

      Cfg.test.map_reduce(m, r, :raw => true, :out => {:inline => 1})
      assert res["results"]
    end
  end

  if Cfg.version > "1.3.0"
    def test_find_and_modify
      Cfg.test << { :a => 1, :processed => false }
      Cfg.test << { :a => 2, :processed => false }
      Cfg.test << { :a => 3, :processed => false }

      Cfg.test.find_and_modify(:query => {}, :sort => [['a', -1]], :update => {"$set" => {:processed => true}})

      assert Cfg.test.find_one({:a => 3})['processed']
    end

    def test_find_and_modify_with_invalid_options
      Cfg.test << { :a => 1, :processed => false }
      Cfg.test << { :a => 2, :processed => false }
      Cfg.test << { :a => 3, :processed => false }

      assert_raises Mongo::OperationFailure do
        Cfg.test.find_and_modify(:blimey => {})
      end
    end
  end

  if Cfg.version >= "1.3.5"
    def test_coll_stats
      Cfg.test << {:n => 1}
      Cfg.test.create_index("n")
      stats = Cfg.test.stats
      assert_equal "#{MONGO_TEST_DB}.test", stats['ns']
    end
  end

  def test_saving_dates_pre_epoch
    begin
      Cfg.test.save({'date' => Time.utc(1600)})
      assert_in_delta Time.utc(1600), Cfg.test.find_one()["date"], 2
    rescue ArgumentError
      # See note in test_date_before_epoch (BSONTest)
    end
  end

  def test_save_symbol_find_string
    Cfg.test.save(:foo => :mike, :foo1 => 'mike')

    assert_equal :mike, Cfg.test.find_one(:foo => :mike)["foo"]
    assert_equal :mike, Cfg.test.find_one("foo" => :mike)["foo"]
    assert_equal 'mike', Cfg.test.find_one("foo" => :mike)["foo1"]

    assert_equal :mike, Cfg.test.find_one(:foo => "mike")["foo"]
    assert_equal :mike, Cfg.test.find_one("foo" => "mike")["foo"]
  end

  def test_limit_and_skip
    10.times do |i|
      Cfg.test.save(:foo => i)
    end

    assert_equal 5, Cfg.test.find({}, :skip => 5).next_document()["foo"]
    assert_equal nil, Cfg.test.find({}, :skip => 10).next_document()

    assert_equal 5, Cfg.test.find({}, :limit => 5).to_a.length

    assert_equal 3, Cfg.test.find({}, :skip => 3, :limit => 5).next_document()["foo"]
    assert_equal 5, Cfg.test.find({}, :skip => 3, :limit => 5).to_a.length
  end

  def test_large_limit
    2000.times do |i|
      Cfg.test.insert("x" => i, "y" => "mongomongo" * 1000)
    end

    assert_equal 2000, Cfg.test.count

    i = 0
    y = 0
    Cfg.test.find({}, :limit => 1900).each do |doc|
      i += 1
      y += doc["x"]
    end

    assert_equal 1900, i
    assert_equal 1804050, y
  end

  def test_small_limit
    Cfg.test.insert("x" => "hello world")
    Cfg.test.insert("x" => "goodbye world")

    assert_equal 2, Cfg.test.count

    x = 0
    Cfg.test.find({}, :limit => 1).each do |doc|
      x += 1
      assert_equal "hello world", doc["x"]
    end

    assert_equal 1, x
  end

  def test_find_with_transformer
    klass       = Struct.new(:id, :a)
    transformer = Proc.new { |doc| klass.new(doc['_id'], doc['a']) }
    cursor      = Cfg.test.find({}, :transformer => transformer)
    assert_equal(transformer, cursor.transformer)
  end

  def test_find_one_with_transformer
    klass       = Struct.new(:id, :a)
    transformer = Proc.new { |doc| klass.new(doc['_id'], doc['a']) }
    id          = Cfg.test.insert('a' => 1)
    doc         = Cfg.test.find_one(id, :transformer => transformer)
    assert_instance_of(klass, doc)
  end

  def test_ensure_index
    Cfg.test.drop_indexes
    Cfg.test.insert("x" => "hello world")
    assert_equal 1, Cfg.test.index_information.keys.count #default index

    Cfg.test.ensure_index([["x", Mongo::DESCENDING]], {})
    assert_equal 2, Cfg.test.index_information.keys.count
    assert Cfg.test.index_information.keys.include? "x_-1"

    Cfg.test.ensure_index([["x", Mongo::ASCENDING]])
    assert Cfg.test.index_information.keys.include? "x_1"

    Cfg.test.ensure_index([["type", 1], ["date", -1]])
    assert Cfg.test.index_information.keys.include? "type_1_date_-1"

    Cfg.test.drop_index("x_1")
    assert_equal 3, Cfg.test.index_information.keys.count
    Cfg.test.drop_index("x_-1")
    assert_equal 2, Cfg.test.index_information.keys.count

    Cfg.test.ensure_index([["x", Mongo::DESCENDING]], {})
    assert_equal 3, Cfg.test.index_information.keys.count
    assert Cfg.test.index_information.keys.include? "x_-1"

    # Make sure that drop_index expires cache properly
    Cfg.test.ensure_index([['a', 1]])
    assert Cfg.test.index_information.keys.include?("a_1")
    Cfg.test.drop_index("a_1")
    assert !Cfg.test.index_information.keys.include?("a_1")
    Cfg.test.ensure_index([['a', 1]])
    assert Cfg.test.index_information.keys.include?("a_1")
    Cfg.test.drop_index("a_1")
  end

end

require 'minitest/spec'

describe "Collection" do
  before do
    Cfg.clear_all
  end

  describe "Grouping" do
    before do
      Cfg.test.save("a" => 1)
      Cfg.test.save("b" => 1)
      @initial = {"count" => 0}
      @reduce_function = "function (obj, prev) { prev.count += inc_value; }"
      @grp_opts = {:initial => @initial, :reduce => BSON::Code.new(@reduce_function, {"inc_value" => 1})}
    end

    it "should fail if missing required options" do
      lambda { Cfg.test.group(:initial => {}) }.must_raise Mongo::MongoArgumentError
      lambda { Cfg.test.group(:reduce => "foo") }.must_raise Mongo::MongoArgumentError
    end

    it "should group results using eval form" do
      @grp_opts[:reduce] = BSON::Code.new(@reduce_function, {"inc_value" => 0.5})
      Cfg.test.group( @grp_opts )[0]["count"].must_equal 1

      @grp_opts[:reduce] = BSON::Code.new(@reduce_function, {"inc_value" => 1})
      Cfg.test.group( @grp_opts )[0]["count"].must_equal 2

      @grp_opts[:reduce] = BSON::Code.new(@reduce_function, {"inc_value" => 2})
      Cfg.test.group( @grp_opts )[0]["count"].must_equal 4
    end

    it "should finalize grouped results" do
      @grp_opts[:finalize] = "function(doc) {doc.f = doc.count + 200; }"
      Cfg.test.group( @grp_opts )[0]["f"].must_equal 202
    end
  end

  describe "Grouping with key" do
    before do
      Cfg.test.save("a" => 1, "pop" => 100)
      Cfg.test.save("a" => 1, "pop" => 100)
      Cfg.test.save("a" => 2, "pop" => 100)
      Cfg.test.save("a" => 2, "pop" => 100)
      @initial = {"count" => 0, "foo" => 1}
      @reduce_function = "function (obj, prev) { prev.count += obj.pop; }"
    end

    it "should group" do
      result = Cfg.test.group(:key => ['a'], :initial => @initial, :reduce => @reduce_function)
      true.must_equal result.all? { |r| r['count'] == 200 }
    end
  end

  describe "Grouping with a key function" do
    before do
      Cfg.test.save("a" => 1)
      Cfg.test.save("a" => 2)
      Cfg.test.save("a" => 3)
      Cfg.test.save("a" => 4)
      Cfg.test.save("a" => 5)
      @initial = {"count" => 0}
      @keyf    = "function (doc) { if(doc.a % 2 == 0) { return {even: true}; } else {return {odd: true}} };"
      @reduce  = "function (obj, prev) { prev.count += 1; }"
    end

    it "should group results" do
      results = Cfg.test.group(:keyf => @keyf, :initial => @initial, :reduce => @reduce).sort {|a, b| a['count'] <=> b['count']}
      true.must_equal results[0]['even'] && results[0]['count'] == 2.0
      true.must_equal results[1]['odd'] && results[1]['count'] == 3.0
    end

    it "should group filtered results" do
      results = Cfg.test.group(:keyf => @keyf, :cond => {:a => {'$ne' => 2}},
        :initial => @initial, :reduce => @reduce).sort {|a, b| a['count'] <=> b['count']}
      true.must_equal results[0]['even'] && results[0]['count'] == 1.0
      true.must_equal results[1]['odd'] && results[1]['count'] == 3.0
    end
  end

  describe "A collection with two records" do
    before do
      @collection = Cfg.db.collection('test-collection')
      @collection.insert({:name => "Jones"})
      @collection.insert({:name => "Smith"})
    end

    it "should have two records" do
      @collection.size.must_equal 2
    end

    it "should remove the two records" do
      @collection.remove()
      @collection.size.must_equal 0
    end

    it "should remove all records if an empty document is specified" do
      @collection.remove({})
      @collection.find.count.must_equal 0
    end

    it "should remove only matching records" do
      @collection.remove({:name => "Jones"})
      @collection.size.must_equal 1
    end
  end

  describe "Drop index " do
    before do
      @collection = Cfg.db.collection('test-collection')
    end

    it "should drop an index" do
      @collection.create_index([['a', Mongo::ASCENDING]])
      assert @collection.index_information['a_1']
      @collection.drop_index([['a', Mongo::ASCENDING]])
      assert_nil @collection.index_information['a_1']
    end

    it "should drop an index which was given a specific name" do
      @collection.create_index([['a', Mongo::DESCENDING]], {:name => 'i_will_not_fear'})
      assert @collection.index_information['i_will_not_fear']
      @collection.drop_index([['a', Mongo::DESCENDING]])
      assert_nil @collection.index_information['i_will_not_fear']
    end

    it "should drops an composite index" do
      @collection.create_index([['a', Mongo::DESCENDING], ['b', Mongo::ASCENDING]])
      assert @collection.index_information['a_-1_b_1']
      @collection.drop_index([['a', Mongo::DESCENDING], ['b', Mongo::ASCENDING]])
      assert_nil @collection.index_information['a_-1_b_1']
    end

    it "should drops an index with symbols" do
      @collection.create_index([['a', Mongo::DESCENDING], [:b, Mongo::ASCENDING]])
      assert @collection.index_information['a_-1_b_1']
      @collection.drop_index([['a', Mongo::DESCENDING], [:b, Mongo::ASCENDING]])
      assert_nil @collection.index_information['a_-1_b_1']
    end
  end

  describe "Creating indexes " do
    before do
      Cfg.db.drop_collection('test-collection')
      @collection = Cfg.db.collection('test-collection')
      @collection.insert({:aaa => 1})
      @geo        = Cfg.db.collection('geo')
    end

    it "should create index using symbols" do
      @collection.create_index :foo, :name => :bar
      @geo.create_index :goo, :name => :baz
      assert @collection.index_information['bar']
      @collection.drop_index :bar
      assert_nil @collection.index_information['bar']
      assert @geo.index_information['baz']
      @geo.drop_index(:baz)
      assert_nil @geo.index_information['baz']
    end

    it "should create a geospatial index" do
      @geo.save({'loc' => [-100, 100]})
      @geo.create_index([['loc', Mongo::GEO2D]])
      assert @geo.index_information['loc_2d']
    end

    it "should create a unique index" do
      @collection.create_index([['a', Mongo::ASCENDING]], :unique => true)
      info = @collection.index_information['a_1']
      assert info
      assert info['unique']
    end

    it "should drop duplicates" do
      @collection.insert({:a => 1})
      @collection.insert({:a => 1})
      assert_equal 2, @collection.count(:query => {:a => 1})
      @collection.create_index([['a', Mongo::ASCENDING]], :unique => true, :dropDups => true)
      assert_equal 1, @collection.find({:a => 1}).count
      assert_equal 1, @collection.find({:a => 1}).count
    end

    it "should drop duplicates with ruby-like drop_dups key" do
      @collection.insert({:a => 1})
      @collection.insert({:a => 1})
      assert_equal 2, @collection.find({:a => 1}).count
      @collection.create_index([['a', Mongo::ASCENDING]], :unique => true, :drop_dups => true)
      assert_equal 1, @collection.find({:a => 1}).count
    end

    it "should drop duplicates with ensure_index and drop_dups key" do
      @collection.insert({:a => 1})
      @collection.insert({:a => 1})
      assert_equal 2, @collection.find({:a => 1}).count
      @collection.ensure_index([['a', Mongo::ASCENDING]], :unique => true, :drop_dups => true)
      assert_equal 1, @collection.find({:a => 1}).count
    end

    it "should create an index in the background" do
      if Cfg.version > '1.3.1'
        @collection.create_index([['b', Mongo::ASCENDING]], :background => true)
        assert @collection.index_information['b_1']['background'] == true
      else
        assert true
      end
    end

    it "should require an array of arrays" do
      assert_raises Mongo::MongoArgumentError do
        @collection.create_index(['c', Mongo::ASCENDING])
      end
    end

    it "should enforce proper index types" do
      assert_raises Mongo::MongoArgumentError do
        @collection.create_index([['c', 'blah']])
      end
    end

    it "should raise an error if index name is greater than 128" do
      assert_raises Mongo::OperationFailure do
        @collection.create_index([['a' * 25, 1], ['b' * 25, 1],
          ['c' * 25, 1], ['d' * 25, 1], ['e' * 25, 1]])
      end
    end

    it "should allow for an alternate name to be specified" do
      @collection.create_index([['a' * 25, 1], ['b' * 25, 1],
        ['c' * 25, 1], ['d' * 25, 1], ['e' * 25, 1]], :name => 'foo_index')
      assert @collection.index_information['foo_index']
    end

    it "should allow creation of multiple indexes" do
      assert @collection.create_index([['a', 1]])
      assert @collection.create_index([['a', 1]])
    end

    describe "with an index created" do
      before do
        @collection.create_index([['b', 1], ['a', 1]])
      end

      it "should return properly ordered index information" do
        assert @collection.index_information['b_1_a_1']
      end
    end
  end

  # describe "Capped collections" do
  #   before do
  #     Cfg.db.drop_collection('log')
  #     @capped = Cfg.db.create_collection('log', :capped => true, :size => 1024)

  #     10.times { |n| @capped.insert({:n => n}) }
  #   end

  #   it "should find using a standard cursor" do
  #     cursor = @capped.find
  #     10.times do
  #       assert cursor.next_document
  #     end
  #     assert_nil cursor.next_document
  #     @capped.insert({:n => 100})
  #     assert_nil cursor.next_document
  #   end

  #   it "should fail tailable cursor on a non-capped collection" do
  #     col = Cfg.db['regular-collection']
  #     col.insert({:a => 1000})
  #     tail = Cursor.new(col, :tailable => true, :order => [['$natural', 1]])
  #     assert_raises OperationFailure do
  #       tail.next_document
  #     end
  #   end

  #   it "should find using a tailable cursor" do
  #     tail = Cursor.new(@capped, :tailable => true, :order => [['$natural', 1]])
  #     10.times do
  #       assert tail.next_document
  #     end
  #     assert_nil tail.next_document
  #     @capped.insert({:n => 100})
  #     assert tail.next_document
  #   end
  # end
end
