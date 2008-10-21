require 'eventmachine'
require 'strscan'

class EventMachine::MemcacheClient < EM::Connection
  include EM::Protocols::LineText2

  Task = Struct.new(:accepts, :options, :callback, :multiline)
  
  private
  def send_store_cmd(cmd, key, value, options, &block)
    value = Marshal.dump(value) unless options[:raw]
    send_data "#{cmd} #{key} #{options[:flags] || 0} #{options[:expire] || 0} #{value.size}#{block ? nil : ' noreply'}\r\n#{value}\r\n"
    @queue << Task.new(['stored', 'not_stored', 'exists', 'not_found'], options, block) if block
  end
  
  public
  def set(key, value, options={}, &block)
    send_store_cmd('set', key, value, options, &block)
  end

  def get(key, options={}, &block)
    send_data "get #{key}\r\n"
    @queue << Task.new(['value'], options, block, true)
  end
  
  def delete(key, options={}, &block)
    send_data "delete #{key}#{block ? nil : 'noreply'}\r\n"
    @queue << Task.new(['deleted','not_found'], options, block) if block
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
  
  %w[ stored not_found deleted ].each do |type| class_eval %[
  def #{type}_handler(task, params)
    task.callback.call if task.callback
  end
  ] end

  def value_handler(task, params, data)
    task.callback.call(task.options[:raw] ? data : (data=="" ? nil : Marshal.load(data)))
  end
  
  def post_init
    @data = ""
    @queue, @multiline = [], nil
    set_delimiter "\r\n"
  end
end

class EventMachine::MemcacheClient < EM::Connection
  def self.settings
    @settings ||= { :host => 'localhost', :port => 11211, :connections => 4, :logging => false }
  end

  %w[ get set delete ].each do |type| class_eval %[
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
  $index = 0
  EventMachine::run {
    EventMachine::MemcacheClient.connection_pool
    
    $index += 1
    EventMachine::add_timer $index, proc {
      EventMachine::MemcacheClient.delete('foo') do
        puts "deleted foo"
      end
    }
    
    $index += 1
    EventMachine::add_timer $index, proc {
      EventMachine::MemcacheClient.get('foo') do |value|
        print "nil => "
        p value
      end
    }
    
    $index += 1
    EventMachine::add_timer $index, proc {
      EventMachine::MemcacheClient.set('foo', "END\r\n", :raw => true) do 
        EventMachine::MemcacheClient.get('foo', :raw => true) do |value|
          print '"END\r\n" => '
          p value
        end
      end
    }
    
    $index += 1
    EventMachine::add_timer $index, proc {
      EventMachine::MemcacheClient.set('foo', "\r\r\n", :raw => true) do 
        EventMachine::MemcacheClient.get('foo', :raw => true) do |value|
          print '"\r\r\n" => '
          p value
        end
      end
    }
    
    $index += 1
    EventMachine::add_timer $index, proc {
      EventMachine::MemcacheClient.set('foo', "\r\r\n\n", :raw => true) do 
        EventMachine::MemcacheClient.get('foo', :raw => true) do |value|
          print '"\r\r\n\n" => '
          p value
        end
      end
    }
    
    $index += 1
    EventMachine::add_timer $index, proc {
      EventMachine::MemcacheClient.set('foo', [1,2]) do 
        EventMachine::MemcacheClient.get('foo') do |value|
          print "[1,2] => "
          p value
          EventMachine::stop
        end
      end
    }
  }
end
