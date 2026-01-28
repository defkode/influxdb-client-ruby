#!/usr/bin/env ruby
# frozen_string_literal: true

# Memory Benchmark for InfluxDB Ruby Client
#
# This benchmark measures memory usage when writing large numbers of points,
# similar to the scenario described in GitHub issue #147.
#
# Usage:
#   ruby examples/memory_benchmark.rb [num_points] [batch_size] [max_queue_size]
#
# Examples:
#   ruby examples/memory_benchmark.rb 100000 1000 0        # 100k points, batch_size=1000, no queue limit
#   ruby examples/memory_benchmark.rb 100000 1000 10000    # 100k points, batch_size=1000, max_queue=10000
#   ruby examples/memory_benchmark.rb 1000000 1000 50000   # 1M points (like issue #147)
#
# Requirements:
#   - InfluxDB running on localhost:8086 (use docker-compose up -d)
#   - Set INFLUXDB_TOKEN environment variable or use default test token

$LOAD_PATH.unshift(File.expand_path('../lib', __dir__))
require 'influxdb-client'

# Memory measurement helper
def memory_mb
  # Use Ruby's built-in memory info
  `ps -o rss= -p #{Process.pid}`.to_i / 1024.0
end

def format_memory(mb)
  format('%.1f MB', mb)
end

# Configuration
NUM_POINTS = (ARGV[0] || 100_000).to_i
BATCH_SIZE = (ARGV[1] || 1_000).to_i
MAX_QUEUE_SIZE = (ARGV[2] || 0).to_i
FLUSH_INTERVAL = 1_000 # 1 second

# InfluxDB connection settings
INFLUXDB_URL = ENV['INFLUXDB_URL'] || 'http://localhost:8086'
INFLUXDB_TOKEN = ENV['INFLUXDB_TOKEN'] || 'my-token'
INFLUXDB_ORG = ENV['INFLUXDB_ORG'] || 'my-org'
INFLUXDB_BUCKET = ENV['INFLUXDB_BUCKET'] || 'my-bucket'

puts '=' * 70
puts 'InfluxDB Ruby Client Memory Benchmark'
puts '=' * 70
puts
puts 'Configuration:'
puts "  Points to write:    #{NUM_POINTS.to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse}"
puts "  Batch size:         #{BATCH_SIZE}"
puts "  Max queue size:     #{MAX_QUEUE_SIZE == 0 ? 'unlimited' : MAX_QUEUE_SIZE}"
puts "  Flush interval:     #{FLUSH_INTERVAL}ms"
puts "  InfluxDB URL:       #{INFLUXDB_URL}"
puts "  Bucket:             #{INFLUXDB_BUCKET}"
puts
puts '-' * 70

# Force GC before starting
GC.start
initial_memory = memory_mb
puts "Initial memory: #{format_memory(initial_memory)}"
puts

# Create write options with batching
write_options = InfluxDB2::WriteOptions.new(
  write_type: InfluxDB2::WriteType::BATCHING,
  batch_size: BATCH_SIZE,
  flush_interval: FLUSH_INTERVAL,
  max_queue_size: MAX_QUEUE_SIZE
)

# Create client
client = InfluxDB2::Client.new(
  INFLUXDB_URL,
  INFLUXDB_TOKEN,
  bucket: INFLUXDB_BUCKET,
  org: INFLUXDB_ORG,
  precision: InfluxDB2::WritePrecision::NANOSECOND,
  use_ssl: false
)

write_api = client.create_write_api(write_options: write_options)

puts 'Writing points...'
puts

start_time = Time.now
peak_memory = initial_memory
last_report_time = start_time
report_interval = 5 # Report every 5 seconds

begin
  NUM_POINTS.times do |i|
    # Create a point with realistic data
    point = InfluxDB2::Point.new(name: 'sensor_data')
                            .add_tag('sensor_id', "sensor_#{i % 100}")
                            .add_tag('location', "location_#{i % 10}")
                            .add_tag('type', 'temperature')
                            .add_field('value', rand * 100)
                            .add_field('quality', rand(100))
                            .time(Time.now.to_i * 1_000_000_000 + i, InfluxDB2::WritePrecision::NANOSECOND)

    write_api.write(data: point)

    # Periodic memory reporting
    if Time.now - last_report_time >= report_interval
      current_memory = memory_mb
      peak_memory = [peak_memory, current_memory].max
      elapsed = Time.now - start_time
      rate = (i + 1) / elapsed

      puts format(
        '  Progress: %6.1f%% | Points: %8d | Memory: %s | Peak: %s | Rate: %6.0f pts/sec',
        (i + 1).to_f / NUM_POINTS * 100,
        i + 1,
        format_memory(current_memory),
        format_memory(peak_memory),
        rate
      )
      last_report_time = Time.now
    end
  end

  puts
  puts 'All points queued. Waiting for flush...'

  # Wait a bit for batches to flush
  pre_close_memory = memory_mb
  peak_memory = [peak_memory, pre_close_memory].max

  puts "Memory before close: #{format_memory(pre_close_memory)}"

  # Close the client (flushes remaining batches)
  client.close!

  post_close_memory = memory_mb
  puts "Memory after close:  #{format_memory(post_close_memory)}"

  # Force GC and measure again
  GC.start
  sleep 0.5
  final_memory = memory_mb

  elapsed_time = Time.now - start_time

  puts
  puts '-' * 70
  puts 'Results:'
  puts '-' * 70
  puts
  puts "  Total time:         #{format('%.2f', elapsed_time)} seconds"
  puts "  Points written:     #{NUM_POINTS.to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse}"
  puts "  Write rate:         #{format('%.0f', NUM_POINTS / elapsed_time)} points/second"
  puts
  puts '  Memory:'
  puts "    Initial:          #{format_memory(initial_memory)}"
  puts "    Peak:             #{format_memory(peak_memory)}"
  puts "    After close:      #{format_memory(post_close_memory)}"
  puts "    After GC:         #{format_memory(final_memory)}"
  puts "    Memory increase:  #{format_memory(peak_memory - initial_memory)} (peak)"
  puts "    Memory retained:  #{format_memory(final_memory - initial_memory)} (after GC)"
  puts
  puts '=' * 70

  # Exit with non-zero if memory retained is suspiciously high (>50MB for 100k points)
  retained = final_memory - initial_memory
  if retained > 50 && NUM_POINTS <= 100_000
    puts 'WARNING: Potential memory leak detected!'
    puts "         Retained #{format_memory(retained)} after GC for only #{NUM_POINTS} points"
    exit 1
  end

rescue Errno::ECONNREFUSED
  puts
  puts 'ERROR: Could not connect to InfluxDB!'
  puts
  puts 'Make sure InfluxDB is running:'
  puts '  docker-compose up -d'
  puts
  puts 'Or set the INFLUXDB_URL environment variable to point to your InfluxDB instance.'
  exit 1
rescue StandardError => e
  puts
  puts "ERROR: #{e.class}: #{e.message}"
  puts e.backtrace.first(5).join("\n")
  exit 1
ensure
  # Client doesn't have a closed? method, so we just attempt to close
  client&.close! rescue nil
end
