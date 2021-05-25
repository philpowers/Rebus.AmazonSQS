using NUnit.Framework;
using Rebus.AmazonSns.Tests;
using Rebus.Tests.Contracts.Transports;

namespace Rebus.AmazonSQS.Tests
{
    [TestFixture, Category(Category.AmazonSqs)]
    public class AmazonSimpleSimpleSend : BasicSendReceive<AmazonSimpleTransportFactory> { }
}
