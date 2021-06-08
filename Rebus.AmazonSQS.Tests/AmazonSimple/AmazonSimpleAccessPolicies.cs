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
        public async Task AutoAttachServicesDisabled_DoesNotAutoAttachServices()
        {
            var name = $"autoattach-disabled-{DateTime.Now:yyyyMMdd-HHmmss}";

            var simpleTransport = _simpleTransportFactory.CreateTransport(
                name,
                TimeSpan.FromMinutes(1),
                new Config.AmazonSimpleTransportOptions { AutoAttachServices = false, DisableAccessPolicyChecks = false });

            // Verify that the services were not attached
            var (snsTransport, _) = simpleTransport.GetInternalTransports();

            var topicArn = await snsTransport.LookupArnForTopicName(name);
            Assert.IsNull(topicArn);
        }

        [Test]
        public async Task AutoAttachServicesWithAccessPolicyChecksEnabled_CreatesSqsAccessPolicyAtInitialization()
        {
            var name = $"autoattach-pce-{DateTime.Now:yyyyMMdd-HHmmss}";

            var simpleTransport = _simpleTransportFactory.CreateTransport(
                name,
                TimeSpan.FromMinutes(1),
                new Config.AmazonSimpleTransportOptions { AutoAttachServices = true, DisableAccessPolicyChecks = false });

            // Verify that the services were attached correctly (SNS -> SQS)
            var (snsTransport, _) = simpleTransport.GetInternalTransports();

            var topicArn = await snsTransport.LookupArnForTopicName(name);
            Assert.NotNull(topicArn);

            var subscriptions = await snsTransport.ListSnsSubscriptions(name);

            Assert.AreEqual(1, subscriptions.Count);
            Assert.True(subscriptions[0].TopicArn.StartsWith("arn:aws:sns") && subscriptions[0].TopicArn.EndsWith($":{name}"));
            Assert.True(subscriptions[0].Endpoint.StartsWith("arn:aws:sqs") && subscriptions[0].Endpoint.EndsWith($":{name}"));

            // Verify that the Access Policy was set property
            var snsHasAccessToSqs = await simpleTransport.CheckSqsAccessPolicy(name, name);
            Assert.True(snsHasAccessToSqs);
        }

        [Test]
        public async Task AutoAttachServicesWithAccessPolicyChecksDisabled_DoesNotCreatesSqsAccessPolicyAtInitialization()
        {
            var name = $"autoattach-pcd-{DateTime.Now:yyyyMMdd-HHmmss}";

            var simpleTransport = _simpleTransportFactory.CreateTransport(
                name,
                TimeSpan.FromMinutes(1),
                new Config.AmazonSimpleTransportOptions { AutoAttachServices = true, DisableAccessPolicyChecks = true });

            var snsHasAccessToSqs = await simpleTransport.CheckSqsAccessPolicy(name, name);
            Assert.False(snsHasAccessToSqs);
        }

        [Test]
        public async Task SendToNewSnsTopicWithAccessPolicyChecksEnabled_CreatesSqsAccessPolicy()
        {
            var timeSuffix = $"{DateTime.Now:yyyyMMdd-HHmmss}";
            var sqsQueueName = $"newtopicaccess-queue-{timeSuffix}";
            var snsTopic = $"newtopicaccess-topic-{timeSuffix}";

            var simpleTransport = _simpleTransportFactory.CreateTransport(
                sqsQueueName,
                TimeSpan.FromMinutes(1),
                new Config.AmazonSimpleTransportOptions { AutoAttachServices = false, DisableAccessPolicyChecks = false });

            var (snsTransport, sqsTransport) = simpleTransport.GetInternalTransports();

            snsTransport.CreateQueue(snsTopic);

            // NOTE: must subscribe using the SNS transport to ensure that the registration itself does not create the
            // access policy that we are checking
            var (_, sqsQueueArn) = sqsTransport.GetQueueId(sqsQueueName);
            await snsTransport.RegisterSubscriber(snsTopic, sqsQueueArn);

            await WithContext(async context =>
            {
                await simpleTransport.Send(snsTopic, MessageWith($"newtopicaccess-content-{timeSuffix}"), context);

                var snsHasAccessToSqs = await simpleTransport.CheckSqsAccessPolicy(sqsQueueName, snsTopic);
                Assert.True(snsHasAccessToSqs);
            });
        }

        [Test]
        public async Task RegisterSubscriptionWithAccessPolicyChecksEnabled_CreatesSqsAccessPolicy()
        {
            var timeSuffix = $"{DateTime.Now:yyyyMMdd-HHmmss}";
            var sqsQueueName = $"registersub-queue-{timeSuffix}";
            var snsTopic = $"registersub-topic-{timeSuffix}";

            var simpleTransport = _simpleTransportFactory.CreateTransport(
                sqsQueueName,
                TimeSpan.FromMinutes(1),
                new Config.AmazonSimpleTransportOptions { AutoAttachServices = false, DisableAccessPolicyChecks = false });

            var (snsTransport, _) = simpleTransport.GetInternalTransports();

            snsTransport.CreateQueue(snsTopic);

            await simpleTransport.RegisterSubscriber(snsTopic, sqsQueueName);

            var snsHasAccessToSqs = await simpleTransport.CheckSqsAccessPolicy(sqsQueueName, snsTopic);
            Assert.True(snsHasAccessToSqs);
        }
    }
}
