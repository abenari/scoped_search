module ScopedSearch

  # The ScopedSearch rest-definition class defines on what fields should be search
  # in the model in what cases
  #
  # A definition can be created by calling the <tt>rest_search</tt> method on
  # an ActiveResource-based class, so you should not create an instance of this
  # class yourself.
  class RestDefinition

    # The Field class specifies a field of a model that is available for searching,
    # in what cases this field should be searched and its default search behavior.
    #
    # Instances of this class are created when calling rest_search in your model
    # class, so you should not create instances of this class yourself.
    class Field

      attr_reader :definition, :field, :key_field, :only_explicit, :complete_value, :ext_method, :operators, :field_type

      # The ActiveRecord-based class that belongs to this field.
      def klass
        definition.klass
      end

      # Returns the column type of this field.
      def type
        return field_type if field_type
        :string
      end

      # Returns true if this field is a datetime-like column
      def datetime?
        [:time, :timestamp].include?(type)
      end

      # Returns true if this field is a date-like column
      def date?
        type == :date
      end

      # Returns true if this field is a date or datetime-like column.
      def temporal?
        datetime? || date?
      end

      # Returns true if this field is numerical.
      # Numerical means either integer, floating point or fixed point.
      def numerical?
        [:integer, :float, :decimal].include?(type)
      end

      # Returns true if this is a textual column.
      def textual?
        [:string, :text].include?(type)
      end

      # Returns true if this is a set.
      def set?
        complete_value.is_a?(Hash)
      end

      def boolean?
        set? && complete_value.values.include?(true)
      end

      # Returns the default search operator for this field.
      def default_operator
        @default_operator ||= case type
          when :string, :text then :like
          else :eq
        end
      end

      def default_order(options)
        return nil if options[:default_order].nil?
        field_name = options[:on] unless options[:rename]
        field_name = options[:rename] if options[:rename]
        order = (options[:default_order].to_s.downcase.include?('desc')) ? "DESC" : "ASC"
        return "#{field_name} #{order}"
      end
      # Initializes a Field instance given the definition passed to the
      # scoped_search call on the ActiveRecord-based model class.
      def initialize(definition, options = {})
        @definition = definition
        @definition.default_order ||= default_order(options)

        case options
        when Symbol, String
          @field = field.to_sym
        when Hash
          @field = options.delete(:on)

          # Set attributes from options hash
          @complete_value   = options[:complete_value]
          @ext_method       = options[:ext_method]
          @operators        = options[:operators]
          @only_explicit    = !!options[:only_explicit]
          @default_operator = options[:default_operator] if options.has_key?(:default_operator)
          @field_type       = options[:field_type] if [:string, :text, :integer, :float, :decimal, :date, :time, :timestamp].include?(options[:field_type])
        end

        # Store this field is the field array
        definition.fields[@field]                  ||= self unless options[:rename]
        definition.fields[options[:rename].to_sym] ||= self if     options[:rename]
        definition.unique_fields                   << self

        # Store definition for alias / aliases as well
        definition.fields[options[:alias].to_sym]                  ||= self   if options[:alias]
        options[:aliases].each { |al| definition.fields[al.to_sym] ||= self } if options[:aliases]
      end
    end

    attr_reader :klass

    # Initializes a ScopedSearch Rest-definition instance.
    # This method will also create the :search_for and :complete_for
    # if it does not yet exist.
    def initialize(klass)
      @klass                 = klass
      @fields                = {}
      @unique_fields         = []
      @profile_fields        = {:default => {}}
      @profile_unique_fields = {:default => []}


      register_search_for! unless klass.respond_to?(:search_for)
      register_complete_for! unless klass.respond_to?(:complete_for)

    end

    attr_accessor :profile, :default_order

    def fields
      @profile ||= :default
      @profile_fields[@profile] ||= {}
    end

    def unique_fields
      @profile ||= :default
      @profile_unique_fields[@profile] ||= []
    end

    # this method return definitions::field object from string
    def field_by_name(name)
      field = fields[name.to_sym]
      field ||= fields[name.to_s.split('.')[0].to_sym]
      field
    end

    # this method is used by the syntax auto completer to suggest operators.
    def operator_by_field_name(name)
      field = field_by_name(name)
      return [] if field.nil?
      return field.operators                        if field.operators
      return ['= ', '!= ']                          if field.set?
      return ['= ', '> ', '< ', '<= ', '>= ','!= '] if field.numerical?
      return ['= ', '!= ', '~ ', '!~ ']             if field.textual?
      return ['= ', '> ', '< ']                     if field.temporal?
      raise ScopedSearch::QueryNotSupported, "could not verify '#{name}' type, this can be a result of a definition error"
    end

    NUMERICAL_REGXP = /^\-?\d+(\.\d+)?$/

    # Returns a list of appropriate fields to search in given a search keyword and operator.
    def default_fields_for(value, operator = nil)

      column_types  = []
      column_types += [:string, :text]                      if [nil, :like, :unlike, :ne, :eq].include?(operator)
      column_types += [:integer, :double, :float, :decimal] if value =~ NUMERICAL_REGXP
      column_types += [:datetime, :date, :timestamp]        if (parse_temporal(value))

      default_fields.select { |field| column_types.include?(field.type) && !field.set? }
    end

    # Try to parse a string as a datetime.
    # Supported formats are Today, Yesterday, Sunday, '1 day ago', '2 hours ago', '3 months ago','Jan 23, 2004'
    # And many more formats that are documented in Ruby DateTime API Doc.
    def parse_temporal(value)
      return Date.current if value =~ /\btoday\b/i
      return 1.day.ago.to_date if value =~ /\byesterday\b/i
      return (eval(value.strip.gsub(/\s+/,'.').downcase)).to_datetime if value =~ /\A\s*\d+\s+\bhours?|minutes?\b\s+\bago\b\s*\z/i
      return (eval(value.strip.gsub(/\s+/,'.').downcase)).to_date if value =~ /\A\s*\d+\s+\b(days?|weeks?|months?|years?)\b\s+\bago\b\s*\z/i
      DateTime.parse(value, true) rescue nil
    end

    # Returns a list of fields that should be searched on by default.
    #
    # Every field will show up in this method's result, except for fields for
    # which the only_explicit parameter is set to true.
    def default_fields
      unique_fields.reject { |field| field.only_explicit }
    end

    # Defines a new search field for this search definition.
    def define(options)
      Field.new(self, options)
    end

    protected

    # Registers the search_for within the class that is used for searching.
    def register_search_for! # :nodoc
      if @klass.ancestors.include?(ActiveResource::Base)
        @klass.class_eval do
          def self.search_for (query)
            search_options = ScopedSearch::RestQueryBuilder.build_query(@rest_search, query)
            search_options
          end
        end
      else
        raise "Class #{klass} is not an ActiveResource derivative!"
      end
    end
  end
end

# Registers the complete_for method within the class that is used for searching.
def register_complete_for! # :nodoc
  @klass.class_eval do
    def self.complete_for (query)
      search_options = ScopedSearch::RestAutoCompleter.auto_complete(@rest_search , query)
      search_options
    end
  end
end
