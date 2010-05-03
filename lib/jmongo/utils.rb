module Mongo

#  module Utils
#    def to_dbobject obj
#      case obj
#      when Array
#        array_to_dblist obj
#      when Hash
#        hash_to_dbobject obj
#      else
#        puts "Un-handled class type [#{obj.class}]"
#        obj
#      end
#    end
#
#    def from_dbobject obj
#      hsh = {}
#      obj.toMap.keySet.each do |key|
#        value = obj.get key
#        puts "value class [#{value.class}]"
#      end
#    end
#
#    private
#
#    def hash_to_dbobject doc
#      obj = JMongo::BasicDBObject.new
#
#      doc.each_pair do |key, value|
#        obj.append(key, to_dbobject(value))
#      end
#
#      obj
#    end
#
#    def array_to_dblist ary
#      list = JMongo::BasicDBList.new
#      
#      ary.each_with_index do |element, index|
#        list.put(index, to_dbobject(value))
#      end
#
#      list
#    end
#
#  end # module Utils

end # module Mongo
