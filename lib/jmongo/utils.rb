# Copyright (C) 2010 Chuck Remes
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
