require "logstash/namespace"
require "logstash/outputs/base"
require "java"
require "rubygems"
require "jdbc/mysql"
require "stud/buffer"

java_import "com.mysql.jdbc.Driver"

# This output will perform a bulk insert of rows into a MySQL table
#
# Example:
#  mysql {
#    username => "root"
#    password => ""
#    table_name => "test"
#
#    # maps column 'col1' to log field 'foo', column 'col2' to log field 'bar', and column 'create_dt' to log field '@timestamp'
#    column_map => ["col1", "foo", "col2", "bar", "create_dt", "@timestamp"]
#
#    # of the form jdbc:mysql://#{host}:#{port}/#{database}
#    jdbc_connection_string => "jdbc:mysql://localhost:3306/test"
#
#    #Ignore errors such as duplicate key warnings, etc - useful if you want to prevent duplicate inserts using a natural key
#    insert_ignore => true
#
#    # Batch insert settings
#    flush_size => 10
#    idle_flush_time => 1
#  }
#
#  Batching is tunable via flush_size (number of inserts to batch at once) and idle_flush_time (maximum time between flushes, in seconds)
#
#  @TODO Add support for SSL
#  @TODO Add support for native ruby mysql (Mysql2)
#  @TODO Actually make use of prepared statements?
#
class LogStash::Outputs::Mysql < LogStash::Outputs::Base
  include Stud::Buffer

  config_name "mysql"
  milestone 1

  # Command line to execute via subprocess. Use dtach or screen to make it non blocking
  config :table_name, :validate => :string, :required => true

  # Of the format column_name => field_name
  config :column_map, :validate => :hash, :required => true, :default => {}

  config :host, :validate => :string, :default => "localhost"
  config :port, :validate => :number, :default => 3306
  config :username, :validate => :string, :default => "root"
  config :password, :validate => :string, :default => ""
  config :database, :validate => :string, :required => true
  config :ssl, :validate => :boolean, :default => true

  config :insert_ignore, :validate => :boolean, :default => false

  # Maximum number of events (rows) to insert at once
  config :flush_size, :validate => :number, :default => 10
  # The amount of time since last flush before a flush is forced.
  config :idle_flush_time, :validate => :number, :default => 1



  public
  def register
    @logger.info("New mysql output", :username => @username,
                 :host => @host, :port => @port, :database => @database)

    buffer_initialize(
      :max_items => @flush_size,
      :max_interval => @idle_flush_timeout,
      :logger => @logger
    )
  end

  def connect()
    if(@connection == nil || @connection.is_closed?)
      jdbc_connection_string = "jdbc:mysql://#{@host}:#{@port}/#{@database}"
      @connection = java.sql.DriverManager.getConnection(jdbc_connection_string, @username, @password)
      if(!@connection.is_closed?)
        logger.info("Successfully connected to #{jdbc_connection_string}")
      end
    end
  end


  public
  def receive(event)
    buffer_receive(event)
    return
  end # def receive

  def flush(events, teardown=false)
    connect
    event_rows = []

    events.each do |event|
      event_columns = []
      @column_map.values.each do |field|
        logger.info("Adding column value", :field => field, :value => event[field])
        event_columns.push(event[field])
      end

      event_rows.push("('#{event_columns.join("','")}')")
    end

    if(@insert_ignore)
      sql = "INSERT IGNORE "
    else
      sql = "INSERT "
    end
    sql += "INTO #{@table_name} (#{@column_map.keys.join(',')}) VALUES #{event_rows.join(",")}"

    @logger.info("Generated SQL", :sql => sql)
    stmt = @connection.createStatement
    stmt.execute(sql)
    logger.info("Number of rows updated", :num_rows => stmt.update_count)
    stmt.close
  end # def flush

  public
  def teardown
    buffer_flush(:final => true)
    @connection.close
    finished
  end # def teardown
end
