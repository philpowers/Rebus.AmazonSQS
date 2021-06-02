using System;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.AmazonSns.Tests;

namespace Rebus.AmazonSQS.Tests.AmazonSNS
{
    [TestFixture, Category(Category.AmazonSns)]
    public class AmazonSnsSubscriptions : SqsFixtureBase
    {
        private AmazonSnsTransportFactory _snsTransportFactory;
        private AmazonSqsTransportFactory _sqsTransportFactory;

        protected override void SetUp()
        {
            _snsTransportFactory = new AmazonSnsTransportFactory();
            _sqsTransportFactory = new AmazonSqsTransportFactory();
        }

        protected override void TearDown()
        {
            base.TearDown();
            _snsTransportFactory.CleanUp(true);
        }

        [Test]
        public async Task RegistrationsForSqsQueueuByArn_IsSuccessful()
        {
            var snsTopicName = $"newsub-topic-{DateTime.Now:yyyyMMdd-HHmmss}";
            var sqsQueueName = $"newsub-queue-{DateTime.Now:yyyyMMdd-HHmmss}";

            var snsTransport = (AmazonSnsTransport)_snsTransportFactory.Create(snsTopicName, TimeSpan.FromMinutes(1));
            var sqsTransport = (AmazonSqsTransport)_sqsTransportFactory.Create(sqsQueueName, TimeSpan.FromMinutes(1));

            var (_, sqsQueueArn) = sqsTransport.GetQueueId(sqsTransport.Address);

            //
            // Verify that normal registrations are works
            await snsTransport.RegisterSubscriber(snsTopicName, sqsQueueArn);

            var subscriptions = await snsTransport.ListSnsSubscriptions(snsTopicName);

            Assert.AreEqual(1, subscriptions.Count);
            Assert.True(subscriptions[0].TopicArn.Contains($":{snsTopicName}"));
            Assert.True(subscriptions[0].Endpoint.Contains($":{sqsQueueName}"));

            //
            // Verify that duplicate registrations work as expected (will not create second actual subscription)
            await snsTransport.RegisterSubscriber(snsTopicName, sqsQueueArn);

            subscriptions = await snsTransport.ListSnsSubscriptions(snsTopicName);

            Assert.AreEqual(1, subscriptions.Count);
            Assert.True(subscriptions[0].TopicArn.Contains($":{snsTopicName}"));
            Assert.True(subscriptions[0].Endpoint.Contains($":{sqsQueueName}"));

            //
            // Verify that unregistering is working correctly
            await snsTransport.UnregisterSubscriber(snsTopicName, sqsQueueArn);

            subscriptions = await snsTransport.ListSnsSubscriptions(snsTopicName);
            Assert.IsEmpty(subscriptions);

            //
            // Verify that attempt to unregister nonexistent subscription does not cause errors
            await snsTransport.UnregisterSubscriber(snsTopicName, sqsQueueArn);

            subscriptions = await snsTransport.ListSnsSubscriptions(snsTopicName);
            Assert.IsEmpty(subscriptions);
        }
    }
}
