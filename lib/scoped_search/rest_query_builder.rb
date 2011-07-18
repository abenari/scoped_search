module ScopedSearch


  class RestQueryBuilder

    attr_reader :ast, :definition, :query, :tokens

    # This method will parse the query string and build query for the rest client using the
    # search query.
    def self.build_query(definition, query)
      return [] if (query.nil? or definition.nil?)

      new(definition, query).build_rest_query
    end

    # Initializes the instance by setting the relevant parameters
    def initialize(definition, query)
      @definition = definition
      @ast        = ScopedSearch::QueryLanguage::Compiler.parse(query)
      @query      = query
      @tokens     = ScopedSearch::QueryLanguage::Compiler.tokenize(query)
    end

    # build rest client query
    def build_rest_query
      parameters = []
      keys       = []
      operators  = []

      # Build SQL WHERE clause using the AST
      find = @ast.to_find_attr(self, definition) do |notification, value|

        # Handle the notifications encountered during the find_attr generation:
        case notification
          when :parameter then parameters << value
          when :key   then keys   << value
          when :operator   then operators   << value
          else raise ScopedSearch::QueryNotSupported, "Cannot handle #{notification.inspect}: #{value.inspect}"
        end
      end

      # Build find_attributes string
      # the format should be "?column[]= op value&column[]= op value"
      parameters.map do |p|
          find = find.sub(/\?/, CGI::escape(p.to_s))
      end
      
      find ||=""
      find = find.gsub(/\s+=\s+/, "+%3D+")
      find = find.gsub(/>=\s+/, ">%3D+")
      find = find.gsub(/<=\s+/, "<%3D+")
      find = find.gsub(/\s+/, "+")
      find_attributes = "?"+find
      return find_attributes
    end

    # A hash that maps the operators of the query language with the corresponding SQL operator.
    OPERATORS = { :eq =>'=',  :ne => '<>', :like => 'LIKE', :unlike => 'NOT LIKE',
                      :gt => '>', :lt =>'<',   :lte => '<=',    :gte => '>=' }

    # Return the SQL operator to use given an operator symbol and field definition.
    #
    # By default, it will simply look up the correct SQL operator in the SQL_OPERATORS
    # hash, but this can be overridden by a database adapter.
    def operator(operator, field)
      raise ScopedSearch::QueryNotSupported, "the operator '#{operator}' is not supported for field type '#{field.type}'" if [:like, :unlike].include?(operator) and !field.textual?
      OPERATORS[operator]
    end

    # Perform a comparison between a field and a Date(Time) value.
    #
    # This function makes sure the date is valid and adjust the comparison in
    # some cases to return more logical results.
    #
    # This function needs a block that can be used to pass other information about the query
    # (parameters that should be escaped, includes) to the query builder.
    #
    # <tt>field</tt>:: The field to test.
    # <tt>operator</tt>:: The operator used for comparison.
    # <tt>value</tt>:: The value to compare the field with.
    def datetime_test(field, operator, value, &block) # :yields: finder_option_type, value

      # Parse the value as a date/time and ignore invalid timestamps
      timestamp = definition.parse_temporal(value)
      return nil unless timestamp

      timestamp = timestamp.to_date if field.date?
      # Check for the case that a date-only value is given as search keyword,
      # but the field is of datetime type. Change the comparison to return
      # more logical results.
      if field.datetime?
        span = 1.minute if(value =~ /\A\s*\d+\s+\bminutes?\b\s+\bago\b\s*\z/i)
        span ||= (timestamp.day_fraction == 0) ? 1.day :  1.hour
        if [:eq, :ne].include?(operator)
          # Instead of looking for an exact (non-)match, look for dates that
          # fall inside/outside the range of timestamps of that day.
          yield(:parameter, timestamp)
          yield(:parameter, timestamp + span)

          field_sql = field.to_find_attr(operator, &block)
          return "#{field_sql} >= ?&#{field_sql} < ?"

        elsif operator == :gt
          # Make sure timestamps on the given date are not included in the results
          # by moving the date to the next day.
          timestamp += span
          operator = :gte

        elsif operator == :lte
          # Make sure the timestamps of the given date are included by moving the
          # date to the next date.
          timestamp += span
          operator = :lt
        end
      end

      # Yield the timestamp and return the SQL test
      yield(:parameter, timestamp)
      "#{field.to_find_attr(operator, &block)} #{operator(operator, field)} ?"
    end

    # Validate the key name is in the set and translate the value to the set value.
    def set_test(field, operator,value, &block)
      set_value = field.complete_value[value.to_sym]
      raise ScopedSearch::QueryNotSupported, "'#{field.field}' should be one of '#{field.complete_value.keys.join(', ')}', but the query was '#{value}'" if set_value.nil?
      raise ScopedSearch::QueryNotSupported, "Operator '#{operator}' not supported for '#{field.field}'" unless [:eq,:ne].include?(operator)
      negate = ''
      if [true,false].include?(set_value)
        negate = 'NOT ' if operator == :ne
        if field.numerical?
          operator =  (set_value == true) ?  :gt : :eq
          set_value = 0
        else
          operator = (set_value == true) ? :ne : :eq
          set_value = false
        end
      end
      yield(:parameter, set_value)
      return "#{negate}(#{field.to_find_attr(operator, &block)} #{self.operator(operator, field)} ?)"
    end

    # Generates a simple SQL test expression, for a field and value using an operator.
    #
    # This function needs a block that can be used to pass other information about the query
    # (parameters that should be escaped, includes) to the query builder.
    #
    # <tt>field</tt>:: The field to test.
    # <tt>operator</tt>:: The operator used for comparison.
    # <tt>value</tt>:: The value to compare the field with.
    def build_find_term(field, operator, value, lhs, &block) # :yields: finder_option_type, value
      
      if [:like, :unlike].include?(operator)
        yield(:parameter, (value !~ /^\%|\*/ && value !~ /\%|\*$/) ? "%#{value}%" : value.tr_s('%*', '%'))
        return "#{field.to_find_attr(operator, &block)} #{self.operator(operator, field)} ?"
      elsif field.temporal?
        return datetime_test(field, operator, value, &block)
      elsif field.set?
        return set_test(field, operator, value, &block)
      else
        value = value.to_i if field.numerical?
        yield(:parameter, value)
        return "#{field.to_find_attr(operator, &block)} #{self.operator(operator, field)} ?"
      end
    end

    # This module gets included into the Field class to add SQL generation.
    module Field

      # Return an SQL representation for this field. Also make sure that
      # the relation which includes the search field is included in the
      # SQL query.
      #
      # This function may yield an :include that should be used in the
      # ActiveRecord::Base#find call, to make sure that the field is available
      # for the SQL query.
      def to_find_attr(operator = nil, &block) # :yields: finder_option_type, value
        field.to_s+"[]="
      end

    end

    # This module contains modules for every AST::Node class to add SQL generation.
    module AST

      # Defines the to_find_attr method for AST LeadNodes
      module LeafNode
        def to_find_attr(builder, definition, &block)
          # for boolean fields allow a short format (example: for 'enabled = true' also allow 'enabled')
          field = definition.field_by_name(value)
          if field && field.boolean?
            key = field.complete_value.map{|k,v| k if v == true}.compact.first
            return builder.set_test(field, :eq, key, &block)
          end

          # Search keywords found without context, just search on all the default fields
          fragments = definition.default_fields_for(value).map do |field|
            builder.build_find_term(field, field.default_operator, value,'', &block)
          end

          case fragments.length
            when 0 then nil
            when 1 then fragments.first
            else "#{fragments.join(' OR ')}"
          end
        end
      end

      # Defines the to_find_attr method for AST operator nodes
      module OperatorNode

              # No explicit field name given, run the operator on all default fields
        def to_default_fields(builder, definition, &block)
          raise ScopedSearch::QueryNotSupported, "Value not a leaf node" unless rhs.kind_of?(ScopedSearch::QueryLanguage::AST::LeafNode)

          # Search keywords found without context, just search on all the default fields
          fragments = definition.default_fields_for(rhs.value, operator).map { |field|
                          builder.build_find_term(field, operator, rhs.value,'', &block) }.compact

          case fragments.length
            when 0 then nil
            when 1 then fragments.first
            else "#{fragments.join(' OR ')}"
          end
        end

        # Explicit field name given, run the operator on the specified field only
        def to_single_field(builder, definition, &block)
          raise ScopedSearch::QueryNotSupported, "Field name not a leaf node" unless lhs.kind_of?(ScopedSearch::QueryLanguage::AST::LeafNode)
          raise ScopedSearch::QueryNotSupported, "Value not a leaf node"      unless rhs.kind_of?(ScopedSearch::QueryLanguage::AST::LeafNode)

          # Search only on the given field.
          field = definition.field_by_name(lhs.value)
          raise ScopedSearch::QueryNotSupported, "Field '#{lhs.value}' not recognized for searching!" unless field
          builder.build_find_term(field, operator, rhs.value,lhs.value, &block)
        end

        # Convert this AST node to an SQL fragment.
        def to_find_attr(builder, definition, &block)
          if children.length == 1
            to_default_fields(builder, definition, &block)
          elsif children.length == 2
            to_single_field(builder, definition, &block)
          else
            raise ScopedSearch::QueryNotSupported, "Don't know how to handle this operator node: #{operator.inspect} with #{children.inspect}!"
          end
        end
      end

      # Defines the to_find_attr method for AST AND operator
      module LogicalOperatorNode
        def to_find_attr(builder, definition, &block)
          fragments = children.map { |c| c.to_find_attr(builder, definition, &block) }.compact.map { |sql| "#{sql}" }
          fragments.empty? ? nil : "#{fragments.join("&")}"
        end
      end
    end

  end
  RestDefinition::Field.send(:include, RestQueryBuilder::Field)
  QueryLanguage::AST::LeafNode.send(:include, RestQueryBuilder::AST::LeafNode)
  QueryLanguage::AST::OperatorNode.send(:include, RestQueryBuilder::AST::OperatorNode)
  QueryLanguage::AST::LogicalOperatorNode.send(:include, RestQueryBuilder::AST::LogicalOperatorNode)
end