
class String
  def to_bson_code
    BSON::Code.new(self)
  end
end

class Symbol
  def to_bson
    BSON::Symbol.new(self.to_s)
  end
end

class Object
  def to_bson
    self
  end
end

class Hash
  def to_bson
    obj = ::JMongo::BasicDBObject.new
    self.each_pair do |key, val|
      obj.put( key.to_s, val.to_bson )
    end
    obj
  end
end

class Array
  def to_bson
    list = Array.new #Java::ComMongodb::DBObject[ary.length].new
    self.each_with_index do |ele, i|
      list[i] = ele.to_bson
    end
    list
  end
end
