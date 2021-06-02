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

        public (AmazonSimpleTransport, AmazonSnsTransport, AmazonSqsTransport) CreateTransports(
            string inputQueueAddress,
            TimeSpan peeklockDuration,
            AmazonSimpleTransportOptions options = null)
        {
            if (options == null)
            {
                options = new AmazonSimpleTransportOptions { AutoAttachServices = true };
            }

            var snsTransport = AmazonSnsTransportFactory.CreateTransport(null, peeklockDuration);
            var sqsTransport = AmazonSqsTransportFactory.CreateTransport(inputQueueAddress, peeklockDuration);

            var simpleTransport = new AmazonSimpleTransport(inputQueueAddress, snsTransport, sqsTransport, options, new ConsoleLoggerFactory(false));
            simpleTransport.Initialize();

            return (simpleTransport, snsTransport, sqsTransport);
        }

        protected override ITransport CreateInstance(string inputQueueAddress, TimeSpan peeklockDuration, AmazonSimpleTransportOptions options)
        {
            var (simpleTransport, _, _) = CreateTransports(inputQueueAddress, peeklockDuration, options);

            return simpleTransport;
        }
    }
}
