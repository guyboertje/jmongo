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
      assert "buffer-monitor", @collection.monitor_collection.name
    end

    it "should insert into the monitorable collection as well" do
      @collection.insert({'field1' => 'some value', 'field2' => 99})
      assert 1, @collection.size
      rec = @collection.find().to_a.first
      assert 1, @collection.monitor_collection.size
      mon_rec = @collection.monitor_collection.find().to_a.first
      apr mon_rec, 'mon rec'
      assert rec['_id'], mon_rec['_id']
      assert 1, mon_rec['action']
    end
  end
end
