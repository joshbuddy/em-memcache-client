require 'eventmachine'
require 'strscan'

class EventMachine::MemcacheClient < EM::Connection
  include EM::Protocols::LineText2
  Task = Struct.new(:accepts, :options, :callback, :multiline)
  
  def set(key, value, options={}, &block)
    send_store_cmd('set', key, value, options, &block)
  end

  def get(key, options={}, &block)
    query = "get #{key}"
    task = Task.new(['value'], options, block, true)
    send_cmd query, task
  end
  
  def delete(key, options={}, &block)
    query = "delete #{key}#{block ? nil : ' noreply'}"
    task = Task.new(['deleted','not_found'], options, block) if block
    send_cmd query, task
  end
  
  def flush_all(&block)
    query = "flush_all#{block ? nil : ' noreply'}"
    task = Task.new(['ok'], {}, block) if block
    send_cmd query, task
  end
  
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
  
  def connection_completed
    @connected = true
    next_task
  end
  
  def unbind
    @connected = false
    reconnect self.class.settings[:host], self.class.settings[:port]
  end
  
  private
  def send_cmd(query, task)
    if @connected
      send_data "#{query}\r\n"
      @queue << task
    else
      @pending << [query, task]
    end
  end
  
  def send_store_cmd(cmd, key, value, options, &block)
    value = Marshal.dump(value) unless options[:raw]
    query = "#{cmd} #{key} #{options[:flags] || 0} #{options[:expire] || 0} #{value.size}#{block ? nil : ' noreply'}\r\n#{value}"
    send_cmd query, Task.new(['stored', 'not_stored', 'exists', 'not_found'], options, block) if block
  end
  
  %w[ stored not_found deleted ok ].each do |type| class_eval %[
  def #{type}_handler(task, params)
    task.callback.call if task.callback
  end
  ] end

  def value_handler(task, params, data)
    task.callback.call(task.options[:raw] ? data : (data=="" ? nil : Marshal.load(data)))
  end

  def post_init
    set_delimiter "\r\n"
    @data = ""
    @pending, @queue, @multiline = [], [], nil
    @connected = false
  end
  
  def next_task
    if @connected and pending = @pending.shift
      query, task = pending
      send_cmd query, task
    end
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

  %w[ get set delete flush_all].each do |type| class_eval %[
    def self.#{type}(*args, &block)
        connection.send("#{type}",*args, &block)
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

if $0 == __FILE__
  EventMachine::run {
    EventMachine::MemcacheClient.set('foo', "END\r\n", :raw => true) do 
      EventMachine::MemcacheClient.get('foo', :raw => true) do |value|
        print '"END\r\n" => '
        p value
      end
    end
  }
end
