using System;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.AmazonSns.Tests;

namespace Rebus.AmazonSQS.Tests.AmazonSNS
{
    [TestFixture, Category(Category.AmazonSimple)]
    public class AmazonSimpleAccessPolicies : AmazonFixtureBase
    {
        private AmazonSimpleTransportFactory _simpleTransportFactory;

        protected override void SetUp()
        {
            _simpleTransportFactory = new AmazonSimpleTransportFactory();
        }

        [Test]
        public async Task RegisterSubscriptionWithAccessPolicyChecksEnabled_CreatesSqsAccessPolicy()
        {
            var timeSuffix = $"{DateTime.Now:yyyyMMdd-HHmmss}";
            var sqsQueueName = $"registersub-queue-{timeSuffix}";
            var snsTopic = $"registersub-topic-{timeSuffix}";

            var (simpleTransport, snsTransport, _) = _simpleTransportFactory.CreateTransports(
                sqsQueueName,
                TimeSpan.FromMinutes(1),
                new Config.AmazonSimpleTransportOptions { AutoAttachServices = false });

            snsTransport.CreateQueue(snsTopic);

            await simpleTransport.RegisterSubscriber(snsTopic, sqsQueueName);

            var snsHasAccessToSqs = await simpleTransport.CheckSqsAccessPolicy(sqsQueueName, snsTopic);
            Assert.True(snsHasAccessToSqs);
        }
    }
}
