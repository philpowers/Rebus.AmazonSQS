using System;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Rebus.AmazonSQS;
using Rebus.AmazonSQS.Tests;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Transport;

namespace Rebus.AmazonSns.Tests
{
    public class AmazonSnsTransportFactory : AmazonTransportFactoryBase<AmazonSNSTransportOptions>
    {
        private const string ConfigFileName = "sns_connectionstring.txt";
        private const string ConfigEnvironmentVariable = "rebus2_asns_connection_string";

        private static ConnectionInfo _connectionInfo;
        internal static ConnectionInfo ConnectionInfo => _connectionInfo ??= ConnectionInfoFromFileOrNull(GetFilePath(ConfigFileName))
                                                                             ?? ConnectionInfoFromEnvironmentVariable(ConfigEnvironmentVariable)
                                                                             ?? Throw($"Could not find Amazon SNS connection info - configure in file '{ConfigFileName}' or environment variable '{ConfigEnvironmentVariable}'");

        public AmazonSnsTransportFactory()
            : base(t => { })
        {
        }

        public static AmazonSnsTransport CreateTransport(string inputQueueAddress, TimeSpan peeklockDuration, AmazonSNSTransportOptions options = null)
        {
            var connectionInfo = ConnectionInfo;

            var amazonSnsConfig = new AmazonSimpleNotificationServiceConfig { RegionEndpoint = connectionInfo.RegionEndpoint };
            var credentials = new BasicAWSCredentials(connectionInfo.AccessKeyId, connectionInfo.SecretAccessKey);

            options ??= new AmazonSNSTransportOptions();
            options.ClientFactory = () => new AmazonSimpleNotificationServiceClient(credentials, amazonSnsConfig);

            var transport = new AmazonSnsTransport(
                options,
                new ConsoleLoggerFactory(false));

            transport.Initialize();
            return transport;
        }

        protected override ITransport CreateInstance(string inputQueueAddress, TimeSpan peeklockDuration, AmazonSNSTransportOptions options)
        {
            return CreateTransport(inputQueueAddress, peeklockDuration, options);
        }
    }
}
