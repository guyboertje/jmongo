require 'minitest/spec'
require './test/test_helper'

Cfg.connection :op_timeout => 10
Cfg.db

describe "Monitorable Pair Collection" do
  before do
    Cfg.clear_all
  end

  describe "monitoring a collection with defaults" do

    before do
      opts = {:monitor => true}
      @collection = Cfg.db.create_collection('buffer', opts)
    end

    it "should have a monitor_collection" do
      assert @collection.monitor_collection
      assert_equal "buffer-monitor", @collection.monitor_collection.name
    end

    it "should insert into the monitorable collection as well" do
      @collection.insert({'field1' => 'some value', 'field2' => 99})
      assert_equal 1, @collection.size
      rec = @collection.find().to_a.first
      assert_equal 1, @collection.monitor_collection.size
      mon_rec = @collection.monitor_collection.find().to_a.first
      assert_equal rec['_id'], mon_rec['_id']
      assert_equal 1, mon_rec['action']
    end

    it "should raise when the subscribe method is called on a normal collection" do
      normal = Cfg.db.create_collection('normal')
      assert normal.respond_to?('monitor_subscribe')
      assert_raises Mongo::MongoArgumentError do
        normal.monitor_subscribe(2)
      end
    end

    it "should raise for invalid options" do
      @stop_callback = lambda{ true }
      @monitored = @collection.monitor_collection
      assert_raises Mongo::MongoArgumentError do
        @monitored.monitor_subscribe({}) #no block
      end
      assert_raises Mongo::MongoArgumentError do
        @monitored.monitor_subscribe({:callback_exit => ''}) {|doc| apr(doc,'doc')}  #not callable
      end
      assert_raises Mongo::MongoArgumentError do
        @monitored.monitor_subscribe({ :callback_exit => @stop_callback }) {|doc| apr(doc,'doc')} #no timeout
      end
      assert_raises Mongo::MongoArgumentError do
        @monitored.monitor_subscribe({:callback_exit => @stop_callback, :exit_check_timeout => 0}) {|doc| apr(doc,'doc')} #timeout not positive float
      end
    end

    it "the monitored collection should have a subscribe method" do
      @stop = 0
      @count = 0
      @values = []
      @stop_callback = lambda{ @stop += 1; true }
      @monitored = @collection.monitor_collection
      assert_equal @collection, @monitored.monitored_collection

      @monitored.monitor_subscribe({:callback_exit => @stop_callback, :exit_check_timeout => 1.0}) do |doc|
        if doc
          id, act = doc.values_at('_id','action')
          @count += (act || 0)
          orig = @monitored.monitored_collection.find_one('_id'=>id)
          @values << orig['field2'] if orig
        end
      end
      @collection.insert({'field1' => 'some value', 'field2' => 99})
      @collection.insert({'field1' => 'some value', 'field2' => 98})

      sleep 1.2
      assert_equal 1, @stop
      assert_equal 2, @count
      assert_equal [99,98], @values
    end
  end
end
