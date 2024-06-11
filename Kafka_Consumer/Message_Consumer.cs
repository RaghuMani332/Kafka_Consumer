using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka_Consumer
{
    public class Message_Consumer
    {
        public void consumeMessage()
        {
            IConfigurationRoot configuration = new ConfigurationBuilder()
                  .SetBasePath(AppContext.BaseDirectory)
                       .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                       .Build();


            var kafkaConfigSection = configuration.GetSection("Kafka");
            String bootstrapServers = kafkaConfigSection["BootstrapServers"];
            String topicName = kafkaConfigSection["TopicName"];
            String groupId = kafkaConfigSection["GroupId"];
            String autoOffsetReset = kafkaConfigSection["AutoOffsetReset"];//Use Earliest for all message,Latest for unreaded message


            ConsumerConfig config = new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = groupId,AutoOffsetReset= (AutoOffsetReset)Enum.Parse(typeof(AutoOffsetReset),autoOffsetReset)};
            IConsumer<String,String> consumer=new ConsumerBuilder<string, string>(config).Build();
            consumer.Subscribe(topicName);

            try
            {
                while(true)
                {
                  var result=  consumer.Consume();
                    Console.WriteLine(result.Message.Value);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }


        }
    }
}
