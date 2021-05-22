using System;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.AmazonSns.Tests;
using Rebus.AmazonSQS.Tests.Extensions;
using Rebus.Tests.Contracts.Extensions;
using Rebus.Transport;

namespace Rebus.AmazonSQS.Tests.AmazonSNS
{
    [TestFixture]
    public class AmazonSnsCreateTopics : SqsFixtureBase
    {
        AmazonSnsTransport _transport;
        string _inputQueueAddress;

        protected override void SetUp()
        {
            _inputQueueAddress = $"queue-{DateTime.Now:yyyyMMdd-HHmmss}";
            _transport = AmazonSnsTransportFactory.CreateTransport(_inputQueueAddress, TimeSpan.FromMinutes(1));
            Using(_transport);
        }

        [TestCase(15)]
        public async Task ItWorks(int messageCount)
        {
            using (var scope = new RebusTransactionScope())
            {
                var context = scope.TransactionContext;

                messageCount.Times(() => _transport.Send(_inputQueueAddress, MessageWith("message-1"), context).Wait());

                await scope.CompleteAsync();
            }

            var receivedMessages = await _transport.ReceiveAll();

            Assert.That(receivedMessages.Count, Is.EqualTo(messageCount));
        }
    }
}
