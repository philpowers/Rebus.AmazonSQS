using System;
using Rebus.AmazonSQS;
using Rebus.AmazonSQS.Tests;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Transport;

namespace Rebus.AmazonSns.Tests
{
    public class AmazonSimpleTransportFactory : AmazonTransportFactoryBase<AmazonSimpleTransportOptions>
    {
        public AmazonSimpleTransportFactory() : base(_ => { })
        {

        }

        public AmazonSimpleTransport CreateTransport(
            string inputQueueAddress,
            TimeSpan peeklockDuration,
            AmazonSimpleTransportOptions options = null)
        {
            if (options == null)
            {
                options = new AmazonSimpleTransportOptions { AutoAttachServices = true };
            }

            var simpleTransport = new AmazonSimpleTransport(
                inputQueueAddress,
                (topicName, snsOptions) => AmazonSnsTransportFactory.CreateTransport(topicName, peeklockDuration),
                (queueName, sqsOptions) => AmazonSqsTransportFactory.CreateTransport(inputQueueAddress, peeklockDuration),
                options,
                new ConsoleLoggerFactory(false));

            simpleTransport.Initialize();

            return simpleTransport;
        }

        protected override ITransport CreateInstance(string inputQueueAddress, TimeSpan peeklockDuration, AmazonSimpleTransportOptions options)
        {
            var simpleTransport = CreateTransport(inputQueueAddress, peeklockDuration, options);

            return simpleTransport;
        }
    }
}
