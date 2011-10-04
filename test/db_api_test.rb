require './test/test_helper'

Cfg.connection :op_timeout => 10
Cfg.db

class DBAPITest < MiniTest::Unit::TestCase
  include Mongo

  def setup
    Cfg.clear_all
    @r1 = {'a' => 1}
    Cfg.coll.insert(@r1) # collection not created until it's used
  end

  def teardown
    Cfg.db.get_last_error
  end

  def test_clear
    assert_equal 1, Cfg.coll.count
    Cfg.coll.remove
    assert_equal 0, Cfg.coll.count
  end

  def test_insert
    _id = Cfg.coll.insert('a' => 2)
    apr _id, "Basic insert"
    assert_kind_of BSON::ObjectId, _id
    assert_kind_of BSON::ObjectId, Cfg.coll.insert('b' => 3)

    assert_equal 3, Cfg.coll.count
    docs = Cfg.coll.find().to_a
    assert_equal 3, docs.length
    assert docs.detect { |row| row['a'] == 1 }
    assert docs.detect { |row| row['a'] == 2 }
    assert docs.detect { |row| row['b'] == 3 }

    Cfg.coll << {'b' => 4}
    docs = Cfg.coll.find().to_a
    assert_equal 4, docs.length
    assert docs.detect { |row| row['b'] == 4 }
  end
end
__END__
  def test_save_ordered_hash
    oh = BSON::OrderedHash.new
    oh['a'] = -1
    oh['b'] = 'foo'

    oid = Cfg.coll.save(oh)
    assert_equal 'foo', Cfg.coll.find_one(oid)['b']

    oh = BSON::OrderedHash['a' => 1, 'b' => 'foo']
    oid = Cfg.coll.save(oh)
    assert_equal 'foo', Cfg.coll.find_one(oid)['b']
  end

  def test_insert_multiple
    ids = Cfg.coll.insert([{'a' => 2}, {'b' => 3}])

    ids.each do |i|
      assert_kind_of BSON::ObjectId, i
    end

    assert_equal 3, Cfg.coll.count
    docs = Cfg.coll.find().to_a
    assert_equal 3, docs.length
    assert docs.detect { |row| row['a'] == 1 }
    assert docs.detect { |row| row['a'] == 2 }
    assert docs.detect { |row| row['b'] == 3 }
  end

  def test_count_on_nonexisting
    Cfg.db.drop_collection('foo')
    assert_equal 0, Cfg.db.collection('foo').count()
  end

  def test_find_simple
    @r2 = Cfg.coll.insert('a' => 2)
    @r3 = Cfg.coll.insert('b' => 3)
    # Check sizes
    docs = Cfg.coll.find().to_a
    assert_equal 3, docs.size
    assert_equal 3, Cfg.coll.count

    # Find by other value
    docs = Cfg.coll.find('a' => @r1['a']).to_a
    assert_equal 1, docs.size
    doc = docs.first
    # Can't compare _id values because at insert, an _id was added to @r1 by
    # the database but we don't know what it is without re-reading the record
    # (which is what we are doing right now).
#   assert_equal doc['_id'], @r1['_id']
    assert_equal doc['a'], @r1['a']
  end

  def test_find_advanced
    Cfg.coll.insert('a' => 2)
    Cfg.coll.insert('b' => 3)

    # Find by advanced query (less than)
    docs = Cfg.coll.find('a' => { '$lt' => 10 }).to_a
    assert_equal 2, docs.size
    assert docs.detect { |row| row['a'] == 1 }
    assert docs.detect { |row| row['a'] == 2 }

    # Find by advanced query (greater than)
    docs = Cfg.coll.find('a' => { '$gt' => 1 }).to_a
    assert_equal 1, docs.size
    assert docs.detect { |row| row['a'] == 2 }

    # Find by advanced query (less than or equal to)
    docs = Cfg.coll.find('a' => { '$lte' => 1 }).to_a
    assert_equal 1, docs.size
    assert docs.detect { |row| row['a'] == 1 }

    # Find by advanced query (greater than or equal to)
    docs = Cfg.coll.find('a' => { '$gte' => 1 }).to_a
    assert_equal 2, docs.size
    assert docs.detect { |row| row['a'] == 1 }
    assert docs.detect { |row| row['a'] == 2 }

    # Find by advanced query (between)
    docs = Cfg.coll.find('a' => { '$gt' => 1, '$lt' => 3 }).to_a
    assert_equal 1, docs.size
    assert docs.detect { |row| row['a'] == 2 }

    # Find by advanced query (in clause)
    docs = Cfg.coll.find('a' => {'$in' => [1,2]}).to_a
    assert_equal 2, docs.size
    assert docs.detect { |row| row['a'] == 1 }
    assert docs.detect { |row| row['a'] == 2 }
  end

  def test_find_sorting
    Cfg.coll.remove
    Cfg.coll.insert('a' => 1, 'b' => 2)
    Cfg.coll.insert('a' => 2, 'b' => 1)
    Cfg.coll.insert('a' => 3, 'b' => 2)
    Cfg.coll.insert('a' => 4, 'b' => 1)

    # Sorting (ascending)
    docs = Cfg.coll.find({'a' => { '$lt' => 10 }}, :sort => [['a', 1]]).to_a
    assert_equal 4, docs.size
    assert_equal 1, docs[0]['a']
    assert_equal 2, docs[1]['a']
    assert_equal 3, docs[2]['a']
    assert_equal 4, docs[3]['a']

    # Sorting (descending)
    docs = Cfg.coll.find({'a' => { '$lt' => 10 }}, :sort => [['a', -1]]).to_a
    assert_equal 4, docs.size
    assert_equal 4, docs[0]['a']
    assert_equal 3, docs[1]['a']
    assert_equal 2, docs[2]['a']
    assert_equal 1, docs[3]['a']

    # Sorting using array of names; assumes ascending order.
    docs = Cfg.coll.find({'a' => { '$lt' => 10 }}, :sort => 'a').to_a
    assert_equal 4, docs.size
    assert_equal 1, docs[0]['a']
    assert_equal 2, docs[1]['a']
    assert_equal 3, docs[2]['a']
    assert_equal 4, docs[3]['a']

    # Sorting using single name; assumes ascending order.
    docs = Cfg.coll.find({'a' => { '$lt' => 10 }}, :sort => 'a').to_a
    assert_equal 4, docs.size
    assert_equal 1, docs[0]['a']
    assert_equal 2, docs[1]['a']
    assert_equal 3, docs[2]['a']
    assert_equal 4, docs[3]['a']

    docs = Cfg.coll.find({'a' => { '$lt' => 10 }}, :sort => [['b', 'asc'], ['a', 'asc']]).to_a
    assert_equal 4, docs.size
    assert_equal 2, docs[0]['a']
    assert_equal 4, docs[1]['a']
    assert_equal 1, docs[2]['a']
    assert_equal 3, docs[3]['a']

    # Sorting using empty array; no order guarantee should not blow up.
    docs = Cfg.coll.find({'a' => { '$lt' => 10 }}, :sort => []).to_a
    assert_equal 4, docs.size

    # Sorting using ordered hash. You can use an unordered one, but then the
    # order of the keys won't be guaranteed thus your sort won't make sense.
    oh = BSON::OrderedHash.new
    oh['a'] = -1
    assert_raises InvalidSortValueError do
      docs = Cfg.coll.find({'a' => { '$lt' => 10 }}, :sort => oh).to_a
    end
  end

  def test_find_limits
    Cfg.coll.insert('b' => 2)
    Cfg.coll.insert('c' => 3)
    Cfg.coll.insert('d' => 4)

    docs = Cfg.coll.find({}, :limit => 1).to_a
    assert_equal 1, docs.size
    docs = Cfg.coll.find({}, :limit => 2).to_a
    assert_equal 2, docs.size
    docs = Cfg.coll.find({}, :limit => 3).to_a
    assert_equal 3, docs.size
    docs = Cfg.coll.find({}, :limit => 4).to_a
    assert_equal 4, docs.size
    docs = Cfg.coll.find({}).to_a
    assert_equal 4, docs.size
    docs = Cfg.coll.find({}, :limit => 99).to_a
    assert_equal 4, docs.size
  end

  def test_find_one_no_records
    Cfg.coll.remove
    x = Cfg.coll.find_one('a' => 1)
    assert_nil x
  end

  def test_drop_collection
    assert Cfg.db.drop_collection(Cfg.coll.name), "drop of collection #{Cfg.coll.name} failed"
    assert !Cfg.db.collection_names.include?(Cfg.coll.name)
  end

  def test_other_drop
    assert Cfg.db.collection_names.include?(Cfg.coll.name)
    Cfg.coll.drop
    assert !Cfg.db.collection_names.include?(Cfg.coll.name)
  end

  def test_collection_names
    names = Cfg.db.collection_names
    assert names.length >= 1
    assert names.include?(Cfg.coll.name)

    coll2 = Cfg.db.collection('test2')
    coll2.insert('a' => 1)      # collection not created until it's used
    names = Cfg.db.collection_names
    assert names.length >= 2
    assert names.include?(Cfg.coll.name)
    assert names.include?('test2')
  ensure
    Cfg.db.drop_collection('test2')
  end

  def test_collections_info
    cursor = Cfg.db.collections_info
    rows = cursor.to_a
    assert rows.length >= 1
    row = rows.detect { |r| r['name'] == Cfg.coll_full_name }
    assert_not_nil row
  end

  def test_collection_options
    Cfg.db.drop_collection('foobar')
    Cfg.db.strict = true

    begin
      coll = Cfg.db.create_collection('foobar', :capped => true, :size => 1024)
      options = coll.options()
      assert_equal 'foobar', options['create']
      assert_equal true, options['capped']
      assert_equal 1024, options['size']
    rescue => ex
     Cfg.db.drop_collection('foobar')
     fail "did not expect exception \"#{ex}\""
    ensure
      Cfg.db.strict = false
    end
  end

  def test_collection_options_are_passed_to_the_existing_ones
    Cfg.db.drop_collection('foobar')

    Cfg.db.create_collection('foobar')

    opts = {:safe => true}
    coll = Cfg.db.create_collection('foobar', opts)
    assert_equal true, coll.safe
  end

  def test_index_information
    assert_equal Cfg.coll.index_information.length, 1

    name = Cfg.coll.create_index('a')
    info = Cfg.db.index_information(Cfg.coll.name)
    assert_equal name, "a_1"
    assert_equal Cfg.coll.index_information, info
    assert_equal 2, info.length

    assert info.has_key?(name)
    assert_equal info[name]["key"], {"a" => 1}
  ensure
    Cfg.db.drop_index(Cfg.coll.name, name)
  end

  def test_index_create_with_symbol
    info = Cfg.coll.index_information
    assert_equal info.length, 1
    name = Cfg.coll.create_index([['a', 1]])
    info = Cfg.db.index_information(Cfg.coll.name)
    assert_equal name, "a_1"
    assert_equal Cfg.coll.index_information, info
    assert_equal 2, info.length

    assert info.has_key?(name)
    assert_equal info[name]['key'], {"a" => 1}
  ensure
    Cfg.db.drop_index(Cfg.coll.name, name)
  end

  def test_multiple_index_cols
    name = Cfg.coll.create_index([['a', DESCENDING], ['b', ASCENDING], ['c', DESCENDING]])
    info = Cfg.db.index_information(Cfg.coll.name)
    assert_equal 2, info.length

    assert_equal name, 'a_-1_b_1_c_-1'
    assert info.has_key?(name)
    assert_equal info[name]['key'], {"a" => -1, "b" => 1, "c" => -1}
  ensure
    Cfg.db.drop_index(Cfg.coll.name, name)
  end

  def test_multiple_index_cols_with_symbols
    name = Cfg.coll.create_index([[:a, DESCENDING], [:b, ASCENDING], [:c, DESCENDING]])
    info = Cfg.db.index_information(Cfg.coll.name)
    assert_equal 2, info.length

    assert_equal name, 'a_-1_b_1_c_-1'
    assert info.has_key?(name)
    assert_equal info[name]['key'], {"a" => -1, "b" => 1, "c" => -1}
  ensure
    Cfg.db.drop_index(Cfg.coll.name, name)
  end

  def test_unique_index
    Cfg.db.drop_collection("blah")
    test = Cfg.db.collection("blah")
    test.create_index("hello")

    test.insert("hello" => "world")
    test.insert("hello" => "mike")
    test.insert("hello" => "world")
    assert !Cfg.db.error?

    Cfg.db.drop_collection("blah")
    test = Cfg.db.collection("blah")
    test.create_index("hello", :unique => true)

    test.insert("hello" => "world")
    test.insert("hello" => "mike")
    test.insert("hello" => "world")
    assert Cfg.db.error?
  end

  def test_index_on_subfield
    Cfg.db.drop_collection("blah")
    test = Cfg.db.collection("blah")

    test.insert("hello" => {"a" => 4, "b" => 5})
    test.insert("hello" => {"a" => 7, "b" => 2})
    test.insert("hello" => {"a" => 4, "b" => 10})
    assert !Cfg.db.error?

    Cfg.db.drop_collection("blah")
    test = Cfg.db.collection("blah")
    test.create_index("hello.a", :unique => true)

    test.insert("hello" => {"a" => 4, "b" => 5})
    test.insert("hello" => {"a" => 7, "b" => 2})
    test.insert("hello" => {"a" => 4, "b" => 10})
    assert Cfg.db.error?
  end

  def test_array
    Cfg.coll.remove
    Cfg.coll.insert({'b' => [1, 2, 3]})
    Cfg.coll.insert({'b' => [1, 2, 3]})
    rows = Cfg.coll.find({}, {:fields => ['b']}).to_a
    assert_equal 2, rows.length
    assert_equal [1, 2, 3], rows[1]['b']
  end

  def test_regex
    regex = /foobar/i
    Cfg.coll << {'b' => regex}
    rows = Cfg.coll.find({}, {:fields => ['b']}).to_a
    if Cfg.version < "1.1.3"
      assert_equal 1, rows.length
      assert_equal regex, rows[0]['b']
    else
      assert_equal 2, rows.length
      assert_equal regex, rows[1]['b']
    end
  end

  def test_regex_multi_line
    if Cfg.version >= "1.9.1"
doc = <<HERE
  the lazy brown
  fox
HERE
      Cfg.coll.save({:doc => doc})
      assert Cfg.coll.find_one({:doc => /n.*x/m})
      Cfg.coll.remove
    end
  end

  def test_non_oid_id
    # Note: can't use Time.new because that will include fractional seconds,
    # which Mongo does not store.
    t = Time.at(1234567890)
    Cfg.coll << {'_id' => t}
    rows = Cfg.coll.find({'_id' => t}).to_a
    assert_equal 1, rows.length
    assert_equal t, rows[0]['_id']
  end

  def test_strict
    assert !Cfg.db.strict?
    Cfg.db.strict = true
    assert Cfg.db.strict?
  ensure
    Cfg.db.strict = false
  end

  def test_strict_access_collection
    Cfg.db.strict = true
    begin
      Cfg.db.collection('does-not-exist')
      fail "expected exception"
    rescue => ex
      assert_equal Mongo::MongoDBError, ex.class
      assert_equal "Collection does-not-exist doesn't exist. Currently in strict mode.", ex.to_s
    ensure
      Cfg.db.strict = false
      Cfg.db.drop_collection('does-not-exist')
    end
  end

  def test_strict_create_collection
    Cfg.db.drop_collection('foobar')
    Cfg.db.strict = true

    begin
      Cfg.db.create_collection('foobar')
      assert true
    rescue => ex
      fail "did not expect exception \"#{ex}\""
    end

    # Now the collection exists. This time we should see an exception.
    assert_raises Mongo::MongoDBError do
      Cfg.db.create_collection('foobar')
    end
    Cfg.db.strict = false
    Cfg.db.drop_collection('foobar')

    # Now we're not in strict mode - should succeed
    Cfg.db.create_collection('foobar')
    Cfg.db.create_collection('foobar')
    Cfg.db.drop_collection('foobar')
  end

  def test_where
    Cfg.coll.insert('a' => 2)
    Cfg.coll.insert('a' => 3)

    assert_equal 3, Cfg.coll.count
    assert_equal 1, Cfg.coll.find('$where' => BSON::Code.new('this.a > 2')).count()
    assert_equal 2, Cfg.coll.find('$where' => BSON::Code.new('this.a > i', {'i' => 1})).count()
  end

  def test_eval
    assert_equal 3, Cfg.db.eval('function (x) {return x;}', 3)

    assert_equal nil, Cfg.db.eval("function (x) {db.test_eval.save({y:x});}", 5)
    assert_equal 5, Cfg.db.collection('test_eval').find_one['y']

    assert_equal 5, Cfg.db.eval("function (x, y) {return x + y;}", 2, 3)
    assert_equal 5, Cfg.db.eval("function () {return 5;}")
    assert_equal 5, Cfg.db.eval("2 + 3;")

    assert_equal 5, Cfg.db.eval(Code.new("2 + 3;"))
    assert_equal 2, Cfg.db.eval(Code.new("return i;", {"i" => 2}))
    assert_equal 5, Cfg.db.eval(Code.new("i + 3;", {"i" => 2}))

    assert_raises OperationFailure do
      Cfg.db.eval("5 ++ 5;")
    end
  end

  def test_hint
    name = Cfg.coll.create_index('a')
    begin
      assert_nil Cfg.coll.hint
      assert_equal 1, Cfg.coll.find({'a' => 1}, :hint => 'a').to_a.size
      assert_equal 1, Cfg.coll.find({'a' => 1}, :hint => ['a']).to_a.size
      assert_equal 1, Cfg.coll.find({'a' => 1}, :hint => {'a' => 1}).to_a.size

      Cfg.coll.hint = 'a'
      assert_equal({'a' => 1}, Cfg.coll.hint)
      assert_equal 1, Cfg.coll.find('a' => 1).to_a.size

      Cfg.coll.hint = ['a']
      assert_equal({'a' => 1}, Cfg.coll.hint)
      assert_equal 1, Cfg.coll.find('a' => 1).to_a.size

      Cfg.coll.hint = {'a' => 1}
      assert_equal({'a' => 1}, Cfg.coll.hint)
      assert_equal 1, Cfg.coll.find('a' => 1).to_a.size

      Cfg.coll.hint = nil
      assert_nil Cfg.coll.hint
      assert_equal 1, Cfg.coll.find('a' => 1).to_a.size
    ensure
      Cfg.coll.drop_index(name)
    end
  end

  def test_hash_default_value_id
    val = Hash.new(0)
    val["x"] = 5
    Cfg.coll.insert val
    id = Cfg.coll.find_one("x" => 5)["_id"]
    assert id != 0
  end

  def test_group
    Cfg.db.drop_collection("test")
    test = Cfg.db.collection("test")

    assert_equal [], test.group(:initial => {"count" => 0}, :reduce => "function (obj, prev) { prev.count++; }")
    assert_equal [], test.group(:initial => {"count" => 0}, :reduce => "function (obj, prev) { prev.count++; }")

    test.insert("a" => 2)
    test.insert("b" => 5)
    test.insert("a" => 1)

    assert_equal 3, test.group(:initial => {"count" => 0},
                      :reduce => "function (obj, prev) { prev.count++; }")[0]["count"]
    assert_equal 3, test.group(:initial => {"count" => 0},
                      :reduce => "function (obj, prev) { prev.count++; }")[0]["count"]
    assert_equal 1, test.group(:cond => {"a" => {"$gt" => 1}},
                      :initial => {"count" => 0}, :reduce => "function (obj, prev) { prev.count++; }")[0]["count"]
    assert_equal 1, test.group(:cond => {"a" => {"$gt" => 1}},
                      :initial => {"count" => 0}, :reduce => "function (obj, prev) { prev.count++; }")[0]["count"]

    finalize = "function (obj) { obj.f = obj.count - 1; }"
    assert_equal 2, test.group(:initial => {"count" => 0},
                      :reduce => "function (obj, prev) { prev.count++; }", :finalize => finalize)[0]["f"]

    test.insert("a" => 2, "b" => 3)
    expected = [{"a" => 2, "count" => 2},
                {"a" => nil, "count" => 1},
                {"a" => 1, "count" => 1}]
    assert_equal expected, test.group(:key => ["a"], :initial => {"count" => 0},
                             :reduce => "function (obj, prev) { prev.count++; }")
    assert_equal expected, test.group(:key => [:a], :initial => {"count" => 0},
                             :reduce => "function (obj, prev) { prev.count++; }")

    assert_raises OperationFailure do
      test.group(:initial => {}, :reduce => "5 ++ 5")
    end
  end

  def test_deref
    Cfg.coll.remove

    assert_equal nil, Cfg.db.dereference(DBRef.new("test", ObjectId.new))
    Cfg.coll.insert({"x" => "hello"})
    key = Cfg.coll.find_one()["_id"]
    assert_equal "hello", Cfg.db.dereference(DBRef.new("test", key))["x"]

    assert_equal nil, Cfg.db.dereference(DBRef.new("test", 4))
    obj = {"_id" => 4}
    Cfg.coll.insert(obj)
    assert_equal obj, Cfg.db.dereference(DBRef.new("test", 4))

    Cfg.coll.remove
    Cfg.coll.insert({"x" => "hello"})
    assert_equal nil, Cfg.db.dereference(DBRef.new("test", nil))
  end

  def test_save
    Cfg.coll.remove

    a = {"hello" => "world"}

    id = Cfg.coll.save(a)
    assert_kind_of ObjectId, id
    assert_equal 1, Cfg.coll.count

    assert_equal id, Cfg.coll.save(a)
    assert_equal 1, Cfg.coll.count

    assert_equal "world", Cfg.coll.find_one()["hello"]

    a["hello"] = "mike"
    Cfg.coll.save(a)
    assert_equal 1, Cfg.coll.count

    assert_equal "mike", Cfg.coll.find_one()["hello"]

    Cfg.coll.save({"hello" => "world"})
    assert_equal 2, Cfg.coll.count
  end

  def test_save_long
    Cfg.coll.remove
    Cfg.coll.insert("x" => 9223372036854775807)
    assert_equal 9223372036854775807, Cfg.coll.find_one()["x"]
  end

  def test_find_by_oid
    Cfg.coll.remove

    Cfg.coll.save("hello" => "mike")
    id = Cfg.coll.save("hello" => "world")
    assert_kind_of ObjectId, id

    assert_equal "world", Cfg.coll.find_one(:_id => id)["hello"]
    Cfg.coll.find(:_id => id).to_a.each do |doc|
      assert_equal "world", doc["hello"]
    end

    id = ObjectId.from_string(id.to_s)
    assert_equal "world", Cfg.coll.find_one(:_id => id)["hello"]
  end

  def test_save_with_object_that_has_id_but_does_not_actually_exist_in_collection
    Cfg.coll.remove

    a = {'_id' => '1', 'hello' => 'world'}
    Cfg.coll.save(a)
    assert_equal(1, Cfg.coll.count)
    assert_equal("world", Cfg.coll.find_one()["hello"])

    a["hello"] = "mike"
    Cfg.coll.save(a)
    assert_equal(1, Cfg.coll.count)
    assert_equal("mike", Cfg.coll.find_one()["hello"])
  end

  def test_collection_names_errors
    assert_raises TypeError do
      Cfg.db.collection(5)
    end
    assert_raises Mongo::InvalidNSName do
      Cfg.db.collection("")
    end
    assert_raises Mongo::InvalidNSName do
      Cfg.db.collection("te$t")
    end
    assert_raises Mongo::InvalidNSName do
      Cfg.db.collection(".test")
    end
    assert_raises Mongo::InvalidNSName do
      Cfg.db.collection("test.")
    end
    assert_raises Mongo::InvalidNSName do
      Cfg.db.collection("tes..t")
    end
  end

  def test_rename_collection
    Cfg.db.drop_collection("foo")
    Cfg.db.drop_collection("bar")
    a = Cfg.db.collection("foo")
    b = Cfg.db.collection("bar")

    assert_raises TypeError do
      a.rename(5)
    end
    assert_raises Mongo::InvalidNSName do
      a.rename("")
    end
    assert_raises Mongo::InvalidNSName do
      a.rename("te$t")
    end
    assert_raises Mongo::InvalidNSName do
      a.rename(".test")
    end
    assert_raises Mongo::InvalidNSName do
      a.rename("test.")
    end
    assert_raises Mongo::InvalidNSName do
      a.rename("tes..t")
    end

    assert_equal 0, a.count()
    assert_equal 0, b.count()

    a.insert("x" => 1)
    a.insert("x" => 2)

    assert_equal 2, a.count()

    a.rename("bar")

    assert_equal 2, a.count()
  end

  # doesn't really test functionality, just that the option is set correctly
  def test_snapshot
    Cfg.db.collection("test").find({}, :snapshot => true).to_a
    assert_raises OperationFailure do
      Cfg.db.collection("test").find({}, :snapshot => true, :sort => 'a').to_a
    end
  end

  def test_encodings
    if RUBY_VERSION >= '1.9'
      ascii = "hello world"
      utf8 = "hello world".encode("UTF-8")
      iso8859 = "hello world".encode("ISO-8859-1")

      if RUBY_PLATFORM =~ /jruby/
        assert_equal "ASCII-8BIT", ascii.encoding.name
      else
        assert_equal "US-ASCII", ascii.encoding.name
      end

      assert_equal "UTF-8", utf8.encoding.name
      assert_equal "ISO-8859-1", iso8859.encoding.name

      Cfg.coll.remove
      Cfg.coll.save("ascii" => ascii, "utf8" => utf8, "iso8859" => iso8859)
      doc = Cfg.coll.find_one()

      assert_equal "UTF-8", doc["ascii"].encoding.name
      assert_equal "UTF-8", doc["utf8"].encoding.name
      assert_equal "UTF-8", doc["iso8859"].encoding.name
    end
  end
end
