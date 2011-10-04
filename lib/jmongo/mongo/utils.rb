# Copyright (C) 2010 Guy Boertje

module Mongo
  module JavaImpl

    module NoImplYetClass
      def raise_not_implemented
        raise NoMethodError, "This method hasn't been implemented yet."
      end
    end

    module Utils
      def raise_not_implemented
        raise NoMethodError, "This method hasn't been implemented yet."
      end

      def trap_raise(ex_class, msg=nil)
        begin
          yield
        rescue => ex
          raise ex_class, msg ? "#{msg} - #{ex.message}" : ex.message
        end
      end

      def prep_id(doc)
        if doc[:_id] && !doc['_id']
          doc['_id'] = doc.delete(:_id)
        end
        doc
      end

      def prep_fields(fields)
        case fields
        when String, Symbol
          {fields => 1}
        when Array
          fields << "_id" if fields.empty?
          Hash[fields.zip( [1]*fields.size )]
        when Hash
          fields
        end
      end

      def prep_sort(key_or_list=nil, direction=nil)
        return if key_or_list.nil?
        if !direction.nil?
          order = [[key_or_list, direction]]
        elsif key_or_list.is_a?(String) || key_or_list.is_a?(Symbol)
          order = [[key_or_list.to_s, 1]]
        else
          order = [key_or_list]
        end
        hord = {}
        order.flatten.each_slice(2){|k,v| hord[k] = sort_value(k,v)}
        to_dbobject(hord)
      end

      def to_dbobject obj
        if obj.respond_to?('to_bson')
          obj.to_bson
        elsif obj.respond_to?(:merge)
          hash_to_dbobject(obj)
        elsif obj.respond_to?(:compact)
          array_to_dblist(obj)
        else
          obj
        end
      end

      def from_dbobject obj
        # for better upstream compatibility make the objects into ruby hash or array

        case obj
        when Java::ComMongodb::BasicDBObject, Java::ComMongodb::CommandResult
          h = obj.hashify
          Hash[h.keys.zip(h.values.map{|v| from_dbobject(v)})]
        when Java::ComMongodb::BasicDBList
          obj.arrayify.map{|v| from_dbobject(v)}
        when Java::JavaUtil::ArrayList
          obj.map{|v| from_dbobject(v)}
        when Java::JavaUtil::Date
          Time.at(obj.get_time/1000.0)
        when Java::OrgBsonTypes::Symbol
          obj.toString.to_sym
        else
          obj
        end
      end

      def sort_value(key, value)
        val = value.to_s.downcase
        return val if val == '2d'
        direction = SortingHash[val]
        return direction if direction != 0
        raise InvalidSortValueError.new(
          "for key: #{key}, #{value} was supplied as a sort direction when acceptable values are: " +
          "Mongo::ASCENDING, 'ascending', 'asc', :ascending, :asc, 1, Mongo::DESCENDING, " +
          "'descending', 'desc', :descending, :desc, -1.")
      end

      SortingHash = Hash.new(0).merge!(
        "ascending" => 1, "asc" => 1, "1" => 1,
        "descending" => -1, "desc" => -1, "-1" => -1
      )

      private

      def hash_to_dbobject doc
        obj = JMongo::BasicDBObject.new
        doc.each_pair do |key, value|
          obj.put(key.to_s, to_dbobject(value))
        end
        obj
      end

      def array_to_dblist ary
        list = [] #Java::ComMongodb::DBObject[ary.length].new
        ary.each_with_index do |ele, i|
          list[i] = to_dbobject(ele)
        end
        list
      end
    end
  end
end
