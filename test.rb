# frozen_string_literal: true

require 'rdkafka'
require 'pry-byebug'

base_config = {
  'bootstrap.servers': 'localhost:9092'
}
config = Rdkafka::Config.new(base_config)

queued_min_message = 5
consumer_config = Rdkafka::Config.new(base_config.merge(
  {
    'group.id': 'fairness-test',
    'auto.offset.reset': 'earliest',
    'queued.min.messages': queued_min_message
  }
))

TOPIC_PREFIX = 'test-topic-'
TOPICS_COUNT = 2
PARTITIONS_PER_TOPIC = 2
EVENTS_PER_PARTITION = 10

topics = Array.new(TOPICS_COUNT) { |i| "#{TOPIC_PREFIX}#{i}" }

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
  puts '---'

  topics.map do |topic|
    puts "Creating topic: #{topic}"

    admin.create_topic(topic, PARTITIONS_PER_TOPIC, 1)
  end.each(&:wait)
  puts '---'
ensure
  admin.close
end

begin
  producer = config.producer

  topics.each do |topic|
    PARTITIONS_PER_TOPIC.times do |partition|
      toppar = "#{topic}/#{partition}"
      puts "Producing messages to #{toppar}"

      EVENTS_PER_PARTITION.times.with_object([]) do |i, handles|

        handles << producer.produce(
          topic:,
          partition:,
          payload: i.to_s
        )
      end.each(&:wait)
    end
  end
  puts '---'
ensure
  producer.close
end

begin
  consumer = consumer_config.consumer

  consumer.subscribe("^#{TOPIC_PREFIX}*")

  consumer.each do |message|
    puts "#{message.topic}/#{message.partition}: #{message.payload}"
  end
ensure
  consumer.close
end
