require "logstash/namespace"
require "logstash/outputs/base"
require "java"
require "rubygems"
require "jdbc/mysql"
require "stud/buffer"


java_import "com.mysql.jdbc.Driver"

# This output will run a command for any matching event.
#
# Example:
# 
#     output {
#       exec {
#         type => abuse
#         command => "iptables -A INPUT -s %{clientip} -j DROP"
#       }
#     }
#
# Run subprocesses via system ruby function
#
# WARNING: if you want it non-blocking you should use & or dtach or other such
# techniques
class LogStash::Outputs::Mysql < LogStash::Outputs::Base
  include Stud::Buffer

  config_name "mysql"
  milestone 1

  # Command line to execute via subprocess. Use dtach or screen to make it non blocking
  config :username, :validate => :string, :required => true
  config :password, :validate => :string, :required => true
  config :jdbc_connection_string, :validate => :string, :required => true
  config :flush_size, :validate => :number, :default => 10

  # The amount of time since last flush before a flush is forced.
  config :idle_flush_time, :validate => :number, :default => 1
  config :table_name, :validate => :string, :required => true

  # Of the format column_name => field_name
  config :column_map, :validate => :hash, :required => true, :default => {}


  public
  def register
    @logger.info("New bsd_queue output", :username => @username,
                 :jdbc_connection_string => @jdbc_connection_string)

    buffer_initialize(
      :max_items => @flush_size,
      :max_interval => @idle_flush_timeout,
      :logger => @logger
    )
    begin
      @connection = java.sql.DriverManager.getConnection(@jdbc_connection_string, @username, @password)
      if(@connection)
        logger.info("Successfully connected")
      end
    rescue Exception => e
      @logger.error(e)
    end
  end


  public
  def receive(event)
    buffer_receive(event)
    return
  end # def receive

  def flush(events, teardown=false)
    event_rows = []

    events.each do |event|
      event_columns = []
      @column_map.values.each do |field|
        logger.info("Adding column value", :field => field, :value => event[field])
        event_columns.push(event[field])
      end

      event_rows.push("('#{event_columns.join("','")}')")
    end

    sql = "INSERT INTO #{@table_name} (#{@column_map.keys.join(',')}) VALUES #{event_rows.join(",")}"
    @logger.info("Generated SQL", :sql => sql)
    stmt = @connection.createStatement
    stmt.execute(sql)
    stmt.close
  end # def flush

  public
  def teardown
    buffer_flush(:final => true)
    @connection.close
    finished
  end # def teardown
end
