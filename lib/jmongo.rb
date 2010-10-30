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

    Dir.glob(search_me).sort.each {|rb| require rb}
  end

  def self.require_all_libs_relative_to( fname, dir = nil )
    require_all_file_extensions_relative_to( fname, 'rb', dir )
  end

  def self.require_all_jars_relative_to( fname, dir = nil )
    require_all_file_extensions_relative_to( fname, 'jar', dir )
  end


  module Utils

    def raise_not_implemented
      raise NoMethodError, "This method hasn't been implemented yet."
    end

    private

  end # module Utils

end  # module Mongo
require 'json' unless defined?(JSON)

Mongo.require_all_jars_relative_to(__FILE__)

# import all of the java packages we'll need into the JMongo namespace
require 'jmongo_jext'

Mongo.require_all_libs_relative_to(__FILE__)

module Mongo
  ASCENDING  =  1
  DESCENDING = -1
  GEO2D      = '2d'

  module Constants
    DEFAULT_BATCH_SIZE = 100
  end

end
