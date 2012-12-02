require File.join(File.dirname(__FILE__), %w[spec_helper])

describe Mongo do
  let(:doc) { { :parent => { :child_1 => '1', :child_2 => 2 } } }

  before do
    connection = Mongo::Connection.new('localhost', 27017, :safe => { :fsync => true })
    @db = connection.db('jmongo_specs')
    @db.collections.select { |c| c.name !~ /system/ }.each { |c| @db.drop_collection c.name }
    @db.create_collection 'docs'
    @collection = @db.collection 'docs'
    @collection.insert doc
  end

  context "find all" do
    subject { @collection.find.to_a }
    it { should have(1).item }
  end

  context "complex query" do
    let(:doc2) { { :_id => { :id_1 => 'x', :id_2 => 5 }, :foo => 'bar' }  }
    let(:query) { { '_id.id_2' => 5, '_id.id_1' => 'x' } }
    before { @collection.insert doc2 }

    describe "failing find_one" do
      subject { @collection.find_one(query) }
      it { should_not be_nil }
    end

    describe "passing find" do
      subject { @collection.find(query).to_a.first }
      it { should_not be_nil }
    end
  end
end
