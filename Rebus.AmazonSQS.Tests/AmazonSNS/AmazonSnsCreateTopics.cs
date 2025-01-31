using System;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.AmazonSns.Tests;
using Rebus.Transport;

namespace Rebus.AmazonSQS.Tests.AmazonSNS
{
    [TestFixture, Category(Category.AmazonSns)]
    public class AmazonSnsCreateTopics : AmazonFixtureBase
    {
        private AmazonSnsTransportFactory _transportFactory;

        protected override void SetUp()
        {
            _transportFactory = new AmazonSnsTransportFactory();
        }

        protected override void TearDown()
        {
            base.TearDown();
            _transportFactory.CleanUp(true);
        }

        [Test]
        public async Task NonQualifiedTopicName_CreatesExpectedTopic()
        {
            var inputTopicName = $"nqtopic-{DateTime.Now:yyyyMMdd-HHmmss}";

            var transport = (AmazonSnsTransport)_transportFactory.Create(inputTopicName, TimeSpan.FromMinutes(1));

            using (var scope = new RebusTransactionScope())
            {
                await transport.Send(inputTopicName, MessageWith("nqtnTest"), scope.TransactionContext);
                scope.Complete();
            }

            var topicArn = await transport.LookupArnForTopicName(inputTopicName);

            Assert.True(topicArn.StartsWith("arn:aws:sns:"));
            Assert.True(topicArn.EndsWith($":{inputTopicName}"));
        }
    }
}
