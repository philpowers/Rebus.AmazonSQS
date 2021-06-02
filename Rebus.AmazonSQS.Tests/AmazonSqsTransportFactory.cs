using System;
using Amazon.Runtime;
using Amazon.SQS;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.AmazonSQS.Tests
{
    public class AmazonSqsTransportFactory : AmazonTransportFactoryBase<AmazonSQSTransportOptions>
    {
        private const string ConfigFileName = "sqs_connectionstring.txt";
        private const string ConfigEnvironmentVariable = "rebus2_asqs_connection_string";

        private const string SqsQueuePrefix = "sqstest";

        private static ConnectionInfo _connectionInfo;
        internal static ConnectionInfo ConnectionInfo => _connectionInfo ??= ConnectionInfoFromFileOrNull(GetFilePath(ConfigFileName))
                                                                             ?? ConnectionInfoFromEnvironmentVariable(ConfigEnvironmentVariable)
                                                                             ?? Throw($"Could not find Amazon SQS connection info - configure in file '{ConfigFileName}' or environment variable '{ConfigEnvironmentVariable}'");

        public AmazonSqsTransportFactory()
            : base(t => ((AmazonSqsTransport)t).DeleteQueue(), SqsQueuePrefix)
        {
        }

        public static AmazonSqsTransport CreateTransport(string inputQueueAddress, TimeSpan peeklockDuration, AmazonSQSTransportOptions options = null)
        {
            var connectionInfo = ConnectionInfo;

            var amazonSqsConfig = new AmazonSQSConfig { RegionEndpoint = connectionInfo.RegionEndpoint };
            var credentials = new BasicAWSCredentials(connectionInfo.AccessKeyId, connectionInfo.SecretAccessKey);

            options ??= new AmazonSQSTransportOptions();
            options.ClientFactory = () => new AmazonSQSClient(credentials, amazonSqsConfig);

            var transport = new AmazonSqsTransport(
                inputQueueAddress,
                new ConsoleLoggerFactory(false),
                new TplAsyncTaskFactory(new ConsoleLoggerFactory(false)),
                options,
                new DefaultRebusTime()
            );

            transport.Initialize(peeklockDuration);
            transport.Purge();
            return transport;
        }

        protected override ITransport CreateInstance(string inputQueueAddress, TimeSpan peeklockDuration, AmazonSQSTransportOptions options)
        {
            return CreateTransport(inputQueueAddress, peeklockDuration, options);
        }
    }
}
