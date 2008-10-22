#!/usr/bin/env ruby

$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'eventmachine'
require 'em/memcache-client'
require 'test/unit'

class ConnectToRealServer < Test::Unit::TestCase
  def setup
    $flush_all = false
    EventMachine::MemcacheClient.clear
    EventMachine::run {
      EventMachine::MemcacheClient.flush_all do
        $flush_all = true
        EventMachine.stop
      end
    }
    assert $flush_all
    EventMachine::MemcacheClient.clear
  end
  
  def test_get_unset_value
    $foo_value = "no value"
    EventMachine::run {
      EventMachine::MemcacheClient.get('foo') do |value|
        $foo_value = value
        EventMachine.stop
      end
    }
    assert_equal nil, $foo_value
  end
  
  def test_set_and_get
    $foo_value = "wrong way"
    EventMachine::run {
      EventMachine::MemcacheClient.set('foo','FOO') do 
        EventMachine::MemcacheClient.get('foo') do |value|
          $foo_value = value
          EventMachine.stop
        end
      end
    }
    assert_equal 'FOO', $foo_value
  end
  
  def test_set_and_get_raw
    $foo_value = "wrong way"
    EventMachine::run {
      EventMachine::MemcacheClient.set('foo','FOO2', :raw => true) do 
        EventMachine::MemcacheClient.get('foo', :raw => true) do |value|
          $foo_value = value
          EventMachine.stop
        end
      end
    }
    assert_equal 'FOO2', $foo_value
  end 
  
  def test_set_rubyobj_and_get_raw
    $foo_value = "wrong way"
    EventMachine::run {
      EventMachine::MemcacheClient.set('foo','FOO3') do 
        EventMachine::MemcacheClient.get('foo', :raw => true) do |value|
          $foo_value = Marshal.load(value)
          EventMachine.stop
        end
      end
    }
    assert_equal 'FOO3', $foo_value
  end
  
  def test_set_array_and_get
    $foo_value = "wrong way"
    EventMachine::run {
      EventMachine::MemcacheClient.set('foo',[1,2,3,"4"]) do 
        EventMachine::MemcacheClient.get('foo') do |value|
          $foo_value = value
          EventMachine.stop
        end
      end
    }
    assert_equal [1,2,3,"4"], $foo_value
  end
 
  def test_delete
    $foo_value = "wrong way"
    $bar_value = "wrong way"
    EventMachine::run {
      EventMachine::MemcacheClient.set('bar',"BAR4") do 
        EventMachine::MemcacheClient.set('foo',"FOO4") do 
          EventMachine::MemcacheClient.delete('foo') do
            EventMachine::MemcacheClient.get('foo') do |value|
              $foo_value = value
              EventMachine::MemcacheClient.get('bar') do |value|
                $bar_value = value
                EventMachine.stop
              end
            end
          end
        end
      end
    }
    assert_equal nil, $foo_value
    assert_equal "BAR4", $bar_value
  end
 
  def test_flush_all
    $foo_value = "wrong way"
    $bar_value = "wrong way"
    EventMachine::run {
      EventMachine::MemcacheClient.set('bar',"BAR4") do 
        EventMachine::MemcacheClient.set('foo',"FOO4") do 
          EventMachine::MemcacheClient.flush_all do
            EventMachine::MemcacheClient.get('foo') do |value|
              $foo_value = value
              EventMachine::MemcacheClient.get('bar') do |value|
                $bar_value = value
                EventMachine.stop
              end
            end
          end
        end
      end
    }
    assert_equal nil, $foo_value
    assert_equal nil, $bar_value
  end

  def test_delete_and_delete
    $foo_value = "wrong way"
    $bar_value = "wrong way"
    $deleted_foo_1st = nil
    $deleted_foo_2nd = nil
    EventMachine::run {
      EventMachine::MemcacheClient.set('bar',"BAR5") do 
        EventMachine::MemcacheClient.set('foo',"FOO5") do 
          EventMachine::MemcacheClient.delete("foo") do |deleted_foo_1st|
            $deleted_foo_1st = deleted_foo_1st
            EventMachine::MemcacheClient.delete("foo") do |deleted_foo_2nd|
              $deleted_foo_2nd = deleted_foo_2nd
              EventMachine::MemcacheClient.get('foo') do |value|
                $foo_value = value
                EventMachine::MemcacheClient.get('bar') do |value|
                  $bar_value = value
                  EventMachine.stop
                end
              end
            end
          end
        end
      end
    }
    assert_equal nil, $foo_value
    assert_equal "BAR5", $bar_value
    assert $deleted_foo_1st
    assert !$deleted_foo_2nd
  end

end
