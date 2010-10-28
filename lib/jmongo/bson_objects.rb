# encoding: UTF-8

# copyright and licensed as per Ruby on Rails
# copied from rails

unless defined? ActiveSupport::BasicObject
  module ActiveSupport
    if defined? ::BasicObject
      # A class with no predefined methods that behaves similarly to Builder's
      # BlankSlate. Used for proxy classes.
      class BasicObject < ::BasicObject
        undef_method :==
        undef_method :equal?

        # Let ActiveSupport::BasicObject at least raise exceptions.
        def raise(*args)
          ::Object.send(:raise, *args)
        end
      end
    else
      class BasicObject #:nodoc:
        instance_methods.each do |m|
          undef_method(m) if m.to_s !~ /(?:^__|^nil\?$|^send$|^object_id$)/
        end
      end
    end
  end
end

# --
# Copyright (C) 2010 Guy Boertje
#
# Licensed under the Apache License, Version 2.0 (the "License");
# ++

# This module is to wrap some of the Java Mongo and BSON classes
# so Mongo ORMs developed with the regular ruby Mongo and BSON
# libraries using is_a? or kind_of? BSON::Xxxx will still work
# without needing to require the BSON lib or fret over the Java Objects
# more classes need defining.

module BSON
  class BasicProxy < ::ActiveSupport::BasicObject
    attr_reader :proxy
    def proxy=(obj)
      raise "Method proxy= called for Abstract Class"
    end
    def method_missing(name, *args, &block)
      raise "Called #{name} on nil proxy" unless @proxy
      @proxy.__send__(name, *args, &block)
    end
  end

  class ObjectId < BasicProxy
    def self.create(obj=nil)
      o = new()
      o.proxy = obj || Java::OrgBsonTypes::ObjectId.new
      o
    end
    def self.from_string(s)
      create(Java::OrgBsonTypes::ObjectId.new(s))
    end

    def proxy=(obj)
      if obj.kind_of?(Java::OrgBsonTypes::ObjectId)
        @proxy = obj
      else
        raise "Can't proxy for #{obj.class}"
      end
    end
    def inspect
      @proxy.toString()
    end
    alias :to_s :inspect
  end

  class Code < String
    # copied verbatim from ruby driver
    # Hash mapping identifiers to their values
    attr_accessor :scope

    # Wrap code to be evaluated by MongoDB.
    #
    # @param [String] code the JavaScript code.
    # @param [Hash] a document mapping identifiers to values, which
    #   represent the scope in which the code is to be executed.
    def initialize(code, scope={})
      super(code)
      @scope = scope
    end

  end


  class OrderedHash < BasicProxy
    #creates a no-default instance
    def self.create(obj)
      ohp = new()
      ohp.proxy = obj
      ohp
    end

    def initialize(*a, &b)
      @proxy = nil
      @default = Hash.new(*a, &b)
    end

    def proxy=(obj)
      if obj.kind_of?(Java::ComMongodb::BasicDBObject)
        @proxy = obj
      else
        raise "Can't proxy for #{obj.class}"
      end
    end

    def default
      @default.default
    end

    def default_proc
      @default.default_proc
    end

    def default=(val_or_proc)
      @default.default = val_or_proc
    end

 # ["ordered_keys=", "invert", "rehash", "replace"] - these methods don't make much sense for a BasicBSONObject and derivative classes

    def ordered_keys
      @proxy.keys
    end

    def index(val)
      ix = @proxy.values.index(val)
      return nil unless ix
      @proxy.key_set.to_a[ix]
    end

    def reject!(&block)
      n = @proxy.size
      @proxy.each do |k,v|
        if yield(k, v)
          delete(k)
        end
      end
      return nil if n == @proxy.size
      @proxy
    end

    def each_key
      keys_ = @proxy.key_set.to_a
      while keys_.length > 0
        yield keys_.shift
      end
    end

    def each_value
      vals = @proxy.values.to_a
      while vals.length > 0
        yield vals.shift
      end
    end

    def fetch(key,default=nil)
      v = @proxy.get(key)
      return v if !!(v)
      return yield(key) if block_given?
      return default unless default.nil?
      @default.fetch(key) #raises index not found exception
    end

    def values_at(*args)
      ret = []
      args.each do |key|
        if @proxy.contains_key?(key)
          ret << @proxy.get(key)
        else
          ret << @default.default
        end
      end
      ret
    end
    alias :indexes :values_at
    alias :indices :values_at

    def shift
      if @proxy.size == 0
        @default.default
      else
        k = @proxy.keys.first
        [k, @proxy.remove_field(k)]
      end
    end

    def []=(key,val)
      k = key.kind_of?(String) ? key : key.to_s
      @proxy.put(k, val)
    end

    def store(key,val)
      k = key.kind_of?(String) ? key : key.to_s
      @proxy.put(k.dup.freeze, val)
    end

    def key?(key)
      @proxy.contains_key?(key)
    end
    alias :has_key? :key?

    def value?(val)
      @proxy.contains_value?(val)
    end
    alias :has_value? :value?

    def values
      @proxy.values.to_a
    end

    def inspect
      if @proxy
        "{" + @proxy.keys.zip(@proxy.values.to_a).map{|k,v| "'#{k}' => #{v}"}.join(', ') + "}"
      else
        @default.inspect
      end
    end
    alias :to_s :inspect

    def delete(key)
      unless @proxy.contains_key?(key)
        block_given? ? yield(key)  : nil
      else
        @proxy.remove_field(key)
      end
    end

    def delete_if(&block)
      @proxy.each do |k,v|
        if yield(k, v)
          delete(k)
        end
      end
    end

  end
end
