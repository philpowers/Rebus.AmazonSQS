using System;
using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Rebus.Activation;
using Rebus.AmazonSQS;
using Rebus.Logging;
using Rebus.Subscriptions;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Transport;
// ReSharper disable ArgumentsStyleNamedExpression

namespace Rebus.Config
{
    public class AmazonSimpleServicesConfig
    {
        /// <summary>
        /// Allows SQS-specific options to be overridden. These options will be used when creating the internal
        /// <see cref="AmazonSqsTransport" /> object. If this is not specified, a default object will be used when
        /// creating the SQS transport
        /// </summary>
        public AmazonSQSTransportOptions SQSTransportOptions { get; set; }

        public AmazonSQSConfig SQSClientConfig { get; set; }

        /// <summary>
        /// Allows SNS-specific options to be overridden. These options will be used when creating the internal
        /// <see cref="AmazonSnsTransport" /> object. If this is not specified, a default object will be used when
        /// creating the SNS transport
        /// </summary>
        public AmazonSNSTransportOptions SNSTransportOptions { get; set; }

        public AmazonSimpleNotificationServiceConfig SNSClientConfig { get; set; }

        public AmazonSimpleServicesConfig(
            AmazonSQSTransportOptions sqsTransportOptions = null,
            AmazonSQSConfig sqsClientConfig = null,
            AmazonSNSTransportOptions snsTransportOptions = null,
            AmazonSimpleNotificationServiceConfig snsClientConfig = null)
        {
            SQSTransportOptions = sqsTransportOptions ?? new AmazonSQSTransportOptions();
            SQSClientConfig = sqsClientConfig ?? new AmazonSQSConfig();
            SNSTransportOptions = snsTransportOptions ?? new AmazonSNSTransportOptions();
            SNSClientConfig = snsClientConfig ?? new AmazonSimpleNotificationServiceConfig();
        }
    }

    /// <summary>
    /// Configuration extensions for the Amazon Simple Queue Service transport
    /// </summary>
    public static class AmazonSimpleConfigurationExtensions
    {
        private const string AmazonSimpleSubText = "The AmazonSimple transport was inserted as the subscriptions storage because SNS has native support for pub/sub messaging";

        /// <summary>
        /// Configures Rebus to use Amazon Simple Queue Service as the message transport
        /// </summary>
        public static void UseAmazonSimple(
            this StandardConfigurer<ITransport> configurer,
            string inputQueueAddress,
            AWSCredentials credentials,
            RegionEndpoint regionEndpoint = null,
            AmazonSimpleTransportOptions transportOptions = null,
            AmazonSimpleServicesConfig servicesConfig = null)
        {
            if (transportOptions == null) transportOptions = new AmazonSimpleTransportOptions();

            Configure(configurer, inputQueueAddress, credentials, transportOptions, GetServicesConfig(servicesConfig, regionEndpoint));
        }

        /// <summary>
        /// Configures Rebus to use Amazon Simple Queue Service as the message transport
        /// </summary>
        public static void UseAmazonSimple(
            this StandardConfigurer<ITransport> configurer,
            string inputQueueAddress,
            string accessKeyId,
            string secretAccessKey,
            RegionEndpoint regionEndpoint = null,
            AmazonSimpleTransportOptions transportOptions = null,
            AmazonSimpleServicesConfig servicesConfig = null)
        {
            if (transportOptions == null) transportOptions = new AmazonSimpleTransportOptions();

            var credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);

            Configure(configurer, inputQueueAddress, credentials, transportOptions, GetServicesConfig(servicesConfig, regionEndpoint));
        }

        private static AmazonSimpleServicesConfig GetServicesConfig(AmazonSimpleServicesConfig servicesConfig, RegionEndpoint regionEndpoint)
        {
            servicesConfig = servicesConfig ?? new AmazonSimpleServicesConfig();
            if (regionEndpoint != null)
            {
                servicesConfig.SQSClientConfig.RegionEndpoint = regionEndpoint;
                servicesConfig.SNSClientConfig.RegionEndpoint = regionEndpoint;
            }

            return servicesConfig;
        }

        private static void Configure(
            StandardConfigurer<ITransport> configurer,
            string inputQueueAddress,
            AWSCredentials credentials,
            AmazonSimpleTransportOptions transportOptions,
            AmazonSimpleServicesConfig servicesConfig)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (inputQueueAddress == null) throw new ArgumentNullException(nameof(inputQueueAddress));
            if (credentials == null) throw new ArgumentNullException(nameof(credentials));
            if (servicesConfig == null) throw new ArgumentNullException(nameof(servicesConfig));

            AmazonSQSConfigurationExtensions.Configure(configurer, false, inputQueueAddress, servicesConfig.SQSTransportOptions);
            AmazonSNSConfigurationExtensions.Configure(configurer, false, inputQueueAddress, servicesConfig.SNSTransportOptions);

            configurer
                .OtherService<AmazonSimpleTransport>()
                .Register(c =>
                {
                    var sqsBus = Config.Configure.With(new BuiltinHandlerActivator())
                        .Transport(t => t.UseAmazonSQS(
                            credentials,
                            servicesConfig.SQSClientConfig,
                            inputQueueAddress,
                            servicesConfig.SQSTransportOptions))
                        .Start();

                    var snsBus = Config.Configure.With(new BuiltinHandlerActivator())
                        .Transport(t => t.UseAmazonSNSAsOneWayClient(
                            credentials,
                            transportOptions: servicesConfig.SNSTransportOptions,
                            clientConfig: servicesConfig.SNSClientConfig))
                        .Start();

                    var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();

                    return new AmazonSimpleTransport(
                        inputQueueAddress,
                        c.Get<AmazonSnsTransport>(),
                        c.Get<AmazonSqsTransport>(),
                        transportOptions,
                        rebusLoggerFactory);
                });

            configurer
                .OtherService<ISubscriptionStorage>()
                .Register(c => c.Get<AmazonSnsTransport>(), description: AmazonSimpleSubText);
        }
    }
}
