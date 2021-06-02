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
        private const string SimpleQueuePrefix = "simpletest";

        public AmazonSimpleTransportFactory() : base(_ => { }, SimpleQueuePrefix)
        {

        }

        public static AmazonSimpleTransport CreateTransport(string inputQueueAddress, TimeSpan peeklockDuration, AmazonSimpleTransportOptions options = null)
        {
            if (options == null)
            {
                options = new AmazonSimpleTransportOptions { AutoAttachServices = true };
            }

            var snsTransport = AmazonSnsTransportFactory.CreateTransport(null, peeklockDuration);
            var sqsTransport = AmazonSqsTransportFactory.CreateTransport(inputQueueAddress, peeklockDuration);

            var simpleTransport = new AmazonSimpleTransport(inputQueueAddress, snsTransport, sqsTransport, options, new ConsoleLoggerFactory(false));
            simpleTransport.Initialize();

            return simpleTransport;
        }

        protected override ITransport CreateInstance(string inputQueueAddress, TimeSpan peeklockDuration, AmazonSimpleTransportOptions options)
        {
            return CreateTransport(inputQueueAddress, peeklockDuration, options);
        }
    }
}
