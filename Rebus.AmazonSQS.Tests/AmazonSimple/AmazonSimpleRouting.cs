using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Tests.Contracts.Extensions;

namespace Rebus.AmazonSQS.Tests.AmazonSNS
{
    [TestFixture, Category(Category.AmazonSimple)]
    public class AmazonSimpleRouting : AmazonFixtureBase
    {
        protected override void SetUp()
        {
        }

        [Test]
        public async Task BusCreatedWithDefaultRouting_RoutesCorrectly()
        {
            var sub1GotEvent = new ManualResetEvent(false);
            var sub2GotEvent = new ManualResetEvent(false);

            var timeSuffix = $"{DateTime.Now:yyyyMMdd-HHmmss}";
            var expectedContent = $"content-{timeSuffix}";

            var publisher = GetBus("publisher");

            var sub1 = GetBus("sub1", async str =>
            {
                if (str == expectedContent)
                {
                    sub1GotEvent.Set();
                }
            });

            var sub2 = GetBus("sub2", async str =>
            {
                if (str == expectedContent)
                {
                    sub2GotEvent.Set();
                }
            });

            await sub1.Bus.Subscribe<string>();
            await sub2.Bus.Subscribe<string>();

            await publisher.Bus.Publish(expectedContent);

            sub1GotEvent.WaitOrDie(TimeSpan.FromSeconds(5));
            sub2GotEvent.WaitOrDie(TimeSpan.FromSeconds(5));
        }

        BuiltinHandlerActivator GetBus(string queueName, Func<string, Task> handlerMethod = null)
        {
            var activator = Using(new BuiltinHandlerActivator());

            var connectionInfo = AmazonSqsTransportFactory.ConnectionInfo;

            var bus = Configure.With(activator)
                .Transport(t => t.UseAmazonSimple(
                    queueName,
                    connectionInfo.AccessKeyId,
                    connectionInfo.SecretAccessKey,
                    connectionInfo.RegionEndpoint,
                    new AmazonSimpleTransportOptions { AutoAttachServices = true }))
                .Start();

            if (handlerMethod != null)
            {
                activator.Handle(handlerMethod);
            }

            return activator;
        }
    }
}
