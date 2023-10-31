# frozen_string_literal: true

require 'rdkafka'
require 'pry-byebug'

base_config = {
  'bootstrap.servers': 'localhost:9092'
}
config = Rdkafka::Config.new(base_config)

# queued_min_message = 1
consumer_config = Rdkafka::Config.new(base_config.merge(
  {
    'group.id': 'fairness-test',
    'auto.offset.reset': 'earliest',
    'message.max.bytes': 10_000,
    'max.partition.fetch.bytes': 1_000,
    'topic.metadata.refresh.interval.ms': 10_000
  }
))

TOPIC_PREFIX = 'test-topic-'
TOPICS_COUNT = 2
PARTITIONS_PER_TOPIC = 2
EVENTS_PER_PARTITION = 5_000
KB = 'x' * 1000
TOTAL_MESSAGES = 1 #TOPICS_COUNT * PARTITIONS_PER_TOPIC * EVENTS_PER_PARTITION

topics = Array.new(TOPICS_COUNT) { |i| "#{TOPIC_PREFIX}#{i}" }
other_topic = "#{TOPIC_PREFIX}100"

begin
  admin = config.admin

  topics.map do |topic|
    puts "Destroying topic: #{topic}"

    admin.delete_topic(topic)
  end.tap do |handles|
    handles.each do |handle|
      handle.wait
    rescue Rdkafka::RdkafkaError
      # nothing
    end
  end
  begin
    puts "Destroying topic: #{other_topic}"
    admin.delete_topic(other_topic)
  rescue Rdkafka::RdkafkaError
    # nothing
  end
  puts '---'

  topics.map do |topic|
    puts "Creating topic: #{topic}"

    admin.create_topic(topic, PARTITIONS_PER_TOPIC, 1)
  end.each(&:wait)
  puts '---'
ensure
  # admin.close
end

begin
  producer = config.producer
  # event = -1

  # topics.each do |topic|
  #   PARTITIONS_PER_TOPIC.times do |partition|
  #     toppar = "#{topic}/#{partition}"
  #     puts "Producing #{EVENTS_PER_PARTITION} messages to #{toppar}"

  #     EVENTS_PER_PARTITION.times.with_object([]) do |_, handles|
  #       payload = "#{event += 1} |#{KB * 8}"
  #       handles << producer.produce(topic:, partition:, payload:)
  #     end.each(&:wait)
  #   end
  # end
  # puts '---'
ensure
  # producer.close
end

begin
  consumer = consumer_config.consumer

  consumer.subscribe("^#{TOPIC_PREFIX}*")
  count = 0
  summary = []

  thr = Thread.new do
    puts "sleeping"
    sleep 10
    puts "Creating #{other_topic}"
    admin.create_topic(other_topic, PARTITIONS_PER_TOPIC, 1)
    producer.produce(topic: other_topic, partition: 1, payload: 'foobar').wait
  end
  sleep 20
  thr.join

  puts "consuming"
  consumer.each do |message|
    # if summary.last.nil? || summary.last.first != [message.topic, message.partition]
    #   summary << [[message.topic, message.partition], 0]
    # end
    # summary.last[1] = summary.last.last + 1
    # count += 1

    puts "#{message.topic}/#{message.partition}: #{message.payload}"
    consumer.store_offset(message)
    break if count >= TOTAL_MESSAGES
  end

  # summary.each do |block|
  #   puts "#{block.first.first}/#{block.first[1]}: #{block.last} events"
  # end
ensure
  consumer.close
  admin.close
  producer.close
end
