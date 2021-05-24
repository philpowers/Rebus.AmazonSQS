using System.Threading.Tasks;
using NUnit.Framework;

namespace Rebus.AmazonSQS.Tests.AmazonSNS
{
    [TestFixture, Category(Category.AmazonSns)]
    public class AmazonSnsWithSqs : SqsFixtureBase
    {
        [Test]
        public async Task RegisterNewSubscriptionForSqsQueueuByName_IsSuccessful()
        {
            Assert.True(false, "Test not yet implemented.");
        }

        [Test]
        public async Task RegisterNewSubscriptionForSqsQueueuByUrl_IsSuccessful()
        {
            Assert.True(false, "Test not yet implemented.");
        }
    }
}
