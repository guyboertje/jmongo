require './test/test_helper'
require 'logger'
Cfg.connection :op_timeout => 10
Cfg.db

class CursorFailTest < MiniTest::Unit::TestCase

  include Mongo

  def setup
    Cfg.test.remove
    Cfg.test.insert('a' => 1)     # collection not created until it's used
    Cfg.test_full_name = "#{MONGO_TEST_DB}.test"
  end

  def test_refill_via_get_more
    assert_equal 1, Cfg.test.count
    1000.times { |i|
      assert_equal 1 + i, Cfg.test.count
      Cfg.test.insert('a' => i)
    }

    assert_equal 1001, Cfg.test.count
    count = 0
    Cfg.test.find.each { |obj|
      count += obj['a']
    }
    assert_equal 1001, Cfg.test.count

    # do the same thing again for debugging
    assert_equal 1001, Cfg.test.count
    count2 = 0
    Cfg.test.find.each { |obj|
      count2 += obj['a']
    }
    assert_equal 1001, Cfg.test.count

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

end
