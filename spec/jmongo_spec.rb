
require File.join(File.dirname(__FILE__), %w[spec_helper])

describe Mongo do
  before :all do
    @connection  = Mongo::Connection.new
    @database    = @connection.db 'jmongo_test'
    @collection  = @database.collection 'test'
    @numbers     = @collection.insert :one => 1, :two => 2
    @booleans    = @collection.insert :true => true, :false => false
    @strings     = @collection.insert :foo => 'bar'
    @arrays      = @collection.insert :array1 => ['1'], :array3 => %w{a b c}
  end

  it "should be able to find one" do
    @collection.find_one.should_not be_nil
  end
end

