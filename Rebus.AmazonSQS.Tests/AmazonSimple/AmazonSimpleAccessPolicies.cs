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
        public async Task AutoAttachServicesWithAccessPolicyChecksEnabled_CreatesSqsAccessPolicyAtInitialization()
        {
            var name = $"autoattach-pce-{($"{DateTime.Now:yyyyMMdd-HHmmss}")}";

            var (simpleTransport, _, _) = _simpleTransportFactory.CreateTransports(
                name,
                TimeSpan.FromMinutes(1),
                new Config.AmazonSimpleTransportOptions { AutoAttachServices = true, DisableAccessPolicyChecks = false });

            var snsHasAccessToSqs = await simpleTransport.CheckSqsAccessPolicy(name, name);
            Assert.True(snsHasAccessToSqs);
        }

        [Test]
        public async Task AutoAttachServicesWithAccessPolicyChecksDisabled_DoesNotCreatesSqsAccessPolicyAtInitialization()
        {
            var name = $"autoattach-pcd-{($"{DateTime.Now:yyyyMMdd-HHmmss}")}";

            var (simpleTransport, _, _) = _simpleTransportFactory.CreateTransports(
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

            var (simpleTransport, snsTransport, sqsTransport) = _simpleTransportFactory.CreateTransports(
                sqsQueueName,
                TimeSpan.FromMinutes(1),
                new Config.AmazonSimpleTransportOptions { AutoAttachServices = false, DisableAccessPolicyChecks = false });

            snsTransport.CreateQueue(snsTopic);

            // NOTE: must subscribe using the SNS transport to ensure that the registration itself does not create the
            // access policy that we are checking
            var (_, sqsQueueArn) = sqsTransport.GetQueueId(sqsQueueName);
            await snsTransport.RegisterSubscriber(snsTopic, sqsQueueArn);

            await WithContext(async context =>
            {
                await simpleTransport.Send(snsTopic, MessageWith($"newtopicaccess-content-{timeSuffix}"), context);

                //var receivedMessage = await simpleTransport.Receive(context, default);
                //Assert.NotNull(receivedMessage);

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

            var (simpleTransport, snsTransport, _) = _simpleTransportFactory.CreateTransports(
                sqsQueueName,
                TimeSpan.FromMinutes(1),
                new Config.AmazonSimpleTransportOptions { AutoAttachServices = false, DisableAccessPolicyChecks = false });

            snsTransport.CreateQueue(snsTopic);

            await simpleTransport.RegisterSubscriber(snsTopic, sqsQueueName);

            var snsHasAccessToSqs = await simpleTransport.CheckSqsAccessPolicy(sqsQueueName, snsTopic);
            Assert.True(snsHasAccessToSqs);
        }
    }
}
