using System;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Persistence.InMem;
using Rebus.Time;

namespace Rebus.AmazonSQS.Tests.AmazonSNS
{
    [TestFixture, Category(Category.AmazonSimple)]
    public class AmazonSimpleRouting : AmazonFixtureBase
    {
        private BuiltinHandlerActivator _activator;

        protected override void SetUp()
        {
            _activator = new BuiltinHandlerActivator();
            Using(_activator);
        }

        [Test]
        public async Task BusCreatedWithDefaultRouting_RoutesCorrectly()
        {
            var timeSuffix = $"{DateTime.Now:yyyyMMdd-HHmmss}";
            var queueName = $"defaultroute-{timeSuffix}";

            var connectionInfo = AmazonSqsTransportFactory.ConnectionInfo;

            _activator.Handle<string>(async (b, c, msg) => {
                Console.WriteLine($"Handled string: '{msg}'");
            });

            var bus = Configure.With(_activator)
                .Transport(t => t.UseAmazonSimple(
                    queueName,
                    connectionInfo.AccessKeyId,
                    connectionInfo.SecretAccessKey,
                    connectionInfo.RegionEndpoint))
                .Start();

            await bus.Publish($"content-{timeSuffix}");
        }
    }
}
