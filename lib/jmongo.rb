unless RUBY_PLATFORM =~ /java/
  error "This gem is only compatible with a java-based ruby environment like JRuby."
  exit 255
end

module Mongo

  # :stopdoc:
  LIBPATH = ::File.expand_path(::File.dirname(__FILE__)) + ::File::SEPARATOR
  PATH = ::File.dirname(LIBPATH) + ::File::SEPARATOR
  # :startdoc:

  # Returns the version string for the library.
  #
  def self.version
    @version ||= File.read(path('version.txt')).strip
  end

  # Returns the library path for the module. If any arguments are given,
  # they will be joined to the end of the libray path using
  # <tt>File.join</tt>.
  #
  def self.libpath( *args, &block )
    rv =  args.empty? ? LIBPATH : ::File.join(LIBPATH, args.flatten)
    if block
      begin
        $LOAD_PATH.unshift LIBPATH
        rv = block.call
      ensure
        $LOAD_PATH.shift
      end
    end
    return rv
  end

  # Returns the lpath for the module. If any arguments are given,
  # they will be joined to the end of the path using
  # <tt>File.join</tt>.
  #
  def self.path( *args, &block )
    rv = args.empty? ? PATH : ::File.join(PATH, args.flatten)
    if block
      begin
        $LOAD_PATH.unshift PATH
        rv = block.call
      ensure
        $LOAD_PATH.shift
      end
    end
    return rv
  end

  # Utility method used to require all files ending with an extension that lie in the
  # directory below this file that has the same name as the filename passed
  # in. Optionally, a specific _directory_ name can be passed in such that
  # the _filename_ does not have to be equivalent to the directory.
  #
  def self.require_all_file_extensions_relative_to( fname, extension, dir = nil )
    dir ||= ::File.basename(fname, '.*')
    search_me = ::File.expand_path(
    ::File.join(::File.dirname(fname), dir, '**', "*.#{extension}"))

    Dir.glob(search_me).sort.each {|rb| puts "requiring #{rb}"; require rb}
  end

  def self.require_all_libs_relative_to( fname, dir = nil )
    require_all_file_extensions_relative_to( fname, 'rb', dir )
  end

  def self.require_all_jars_relative_to( fname, dir = nil )
    require_all_file_extensions_relative_to( fname, 'jar', dir )
  end


  module Utils
    def to_dbobject obj
      case obj
      when Array
        array_to_dblist obj
      when Hash
        hash_to_dbobject obj
      else
        puts "Un-handled class type [#{obj.class}]"
        obj
      end
    end

    def from_dbobject obj
      hsh = {}
      obj.toMap.keySet.each do |key|
        value = obj.get key
        #        puts "classes, key [#{key.class}], value [#{value.class}]"
        #        puts "values, key [#{key}], value [#{value}]"

        case value
          # when I need to manipulate ObjectID objects, they should be
          # processed here and wrapped in a ruby obj with the right api
        when JMongo::BasicDBObject, JMongo::BasicDBList
          hsh[key] = from_dbobject value
        else
          hsh[key] = value
        end
      end
      hsh
      #obj
    end

    def raise_not_implemented
      raise NoMethodError, "This method hasn't been implemented yet."
    end

    private

    def hash_to_dbobject doc
      obj = JMongo::BasicDBObject.new

      doc.each_pair do |key, value|
        obj.append(key, to_dbobject(value))
      end

      obj
    end

    def array_to_dblist ary
      list = JMongo::BasicDBList.new

      ary.each_with_index do |element, index|
        list.put(index, to_dbobject(value))
      end

      list
    end

  end # module Utils

end  # module Mongo

Mongo.require_all_libs_relative_to(__FILE__)

require 'java'
Mongo.require_all_jars_relative_to(__FILE__)

# import all of the java packages we'll need into the JMongo namespace
module JMongo
  import com.mongodb.BasicDBList
  import com.mongodb.BasicDBObject
  import com.mongodb.ByteDecoder
  import com.mongodb.ByteEncoder
  import com.mongodb.Bytes
  import com.mongodb.DB
  import com.mongodb.DBCollection
  import com.mongodb.DBCursor
  import com.mongodb.DBObject
  import com.mongodb.Mongo
  import com.mongodb.MongoOptions
  import com.mongodb.ServerAddress
end


module Mongo
  ASCENDING  =  1
  DESCENDING = -1

  module Constants

    DEFAULT_BATCH_SIZE = 100
  end

end
