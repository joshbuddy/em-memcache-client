require 'eventmachine'

class EventMachine::MemcacheClient < EM::Connection
  include EM::Protocols::LineText2
  Task = Struct.new(:query, :accepts, :options, :callback, :multiline)
  
  def set(key, value, options={}, &block)
    send_store_cmd('set', key, value, options, &block)
  end

  def get(key, options={}, &block)
    query = "get #{key}"
    task = Task.new(query, ['value'], options, block, true)
    send_cmd task
  end
  
  def delete(key, options={}, &block)
    query = "delete #{key}#{block ? nil : ' noreply'}"
    task = Task.new(query, (block ? ['deleted','not_found'] : nil), options, block) 
    send_cmd task
  end
  
  def flush_all(&block)
    query = "flush_all#{block ? nil : ' noreply'}"
    task = Task.new(query, (block ? ['ok'] : nil), {}, block)
    send_cmd task
  end
  
  private
  
  def receive_line(line)
    if @multiline || (@queue.size>0 && @queue.first.multiline && line=='END')
      @multiline ||= ['value', @queue.shift, [0]]
      if line=='END' && (@data.size >= @multiline[2][2].to_i)
        cmd, task, params = @multiline
        send("#{cmd}_handler", task, params, @data[0, @multiline[2][2].to_i])
        @multiline, @data = nil, ""
      else
        @data << "#{line}\r\n"
      end
      
    elsif @queue.size>0
      task = @queue.shift
      cmd, *params = line.split(' ')
      cmd.downcase!
      if task.accepts.include?(cmd)
        if task.multiline
          @multiline = [cmd, task, params]
        else
          send("#{cmd}_handler", task, params)
        end
      end
    end
  end
  
  def send_cmd(task)
    if @connected
      send_data "#{task.query}\r\n"
      @queue << task if task.accepts
    else
      @pending << task
    end
  end
  
  def send_store_cmd(cmd, key, value, options, &block)
    value = Marshal.dump(value) unless options[:raw]
    query = "#{cmd} #{key} #{options[:flags] || 0} #{options[:expire] || 0} #{value.size}#{block ? nil : ' noreply'}\r\n#{value}"
    send_cmd Task.new(query, ['stored', 'not_stored', 'exists', 'not_found'], options, block) if block
  end
  
  %w[ stored not_found deleted ok ].each do |type| class_eval %[
  def #{type}_handler(task, params)
    task.callback.call if task.callback
  end
  ] end

  def value_handler(task, params, data)
    task.callback.call(task.options[:raw] ? data : (data=="" ? nil : Marshal.load(data)))
  end

  def next_task
    if @connected and pending_task = @pending.shift
      send_cmd pending_task
    end
  end
  
  # EM::Connection event
  def post_init
    set_delimiter "\r\n"
    @data = ""
    @pending, @queue, @multiline = [], [], nil
    @connected = false
  end
  
  public
  
  # EM::Connection event
  def connection_completed
    @connected = true
    next_task
  end
  
  # unbind and auto recconect
  # EM::Connection event
  def unbind
    @connected = false
    reconnect self.class.settings[:host], self.class.settings[:port]
  end
end

class EventMachine::MemcacheClient < EM::Connection
  def self.clear
    @settings = nil
    @n = nil
    @connection_pool = nil
  end
  
  def self.settings
    @settings ||= { :host => 'localhost', :port => 11211, :connections => 4, :logging => false }
  end

  %w[ get set delete flush_all ].each do |type| class_eval %[
    def self.#{type}(*args, &block)
        connection.send("#{type}",*args, &block)
    end
  ] end
  
  %w[ flush_all ].each do |type| class_eval %[
    def self.#{type}(*args, &block)
      responses = 0
      connection_pool.each do |conn|
        conn.send("#{type}",*args) do
          responses += 1
          block.call if block and responses == @connection_pool.size
        end
      end
    end
  ] end

  def self.connection
    @n ||= 0
    connection = connection_pool[@n]
    @n = 0 if (@n+=1) >= connection_pool.size
    connection
  end
  
  def self.connection_pool
    @connection_pool ||= (1..settings[:connections]).map{ EM::connect(settings[:host], settings[:port], self) }
  end
end
