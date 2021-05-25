using System;
using Rebus.AmazonSQS;
using Rebus.AmazonSQS.Tests;
using Rebus.Config;
using Rebus.Transport;

namespace Rebus.AmazonSns.Tests
{
    public class AmazonSimpleTransportFactory : AmazonTransportFactoryBase<AmazonSimpleTransportOptions>
    {
        public AmazonSimpleTransportFactory() : base(_ => { })
        {

        }

        public static AmazonSimpleTransport CreateTransport(string inputQueueAddress, TimeSpan peeklockDuration, AmazonSimpleTransportOptions options = null)
        {
            var snsTransport = AmazonSnsTransportFactory.CreateTransport(null, peeklockDuration);
            var sqsTransport = AmazonSqsTransportFactory.CreateTransport(inputQueueAddress, peeklockDuration);

            return new AmazonSimpleTransport(snsTransport, sqsTransport);
        }

        protected override ITransport CreateInstance(string inputQueueAddress, TimeSpan peeklockDuration, AmazonSimpleTransportOptions options)
        {
            return CreateTransport(inputQueueAddress, peeklockDuration, options);
        }
    }
}
