using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.AmazonSns.Tests;

namespace Rebus.AmazonSQS.Tests.AmazonSNS
{
    [TestFixture, Category(Category.AmazonSns)]
    public class AmazonSnsSubscriptions : SqsFixtureBase
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
        public async Task RegisterNewSubscriptionForSqsQueueuByArn_IsSuccessful()
        {
            Assert.True(false, "Test not yet implemented.");
        }

        [Test]
        public async Task RegisterNewSubscriptionForSqsQueueuByUrl_IsSuccessful()
        {
            Assert.True(false, "Test not yet implemented.");
        }

        [Test]
        public async Task RegisterDuplicateSubscriptionForSqsQueue_MaintainsExistingSubscription()
        {
            Assert.True(false, "Test not yet implemented.");
        }

        [Test]
        public async Task UnregisterSubscriptionForSqsQueue_RemovesSubscription()
        {
            Assert.True(false, "Test not yet implemented.");
        }

        [Test]
        public async Task UnregisterNonExistentSubscription_NoError()
        {
            Assert.True(false, "Test not yet implemented.");
        }
    }
}
