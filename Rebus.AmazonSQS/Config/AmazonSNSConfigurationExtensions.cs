using System;
using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Rebus.AmazonSQS;
using Rebus.Exceptions;
using Rebus.Logging;
using Rebus.Subscriptions;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Transport;
// ReSharper disable ArgumentsStyleNamedExpression

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for the Amazon Simple Queue Service transport
    /// </summary>
    public static class AmazonSNSConfigurationExtensions
    {
        private const string AmazonSnsSubText = "The AmazonSNS transport was inserted as the subscriptions storage because it has native support for pub/sub messaging";

        /// <summary>
        /// Configures Rebus to use Amazon Simple Queue Service as the message transport
        /// </summary>
        public static void UseAmazonSNS(
            this StandardConfigurer<ITransport> configurer,
            string defaultTopicAddress,
            AWSCredentials credentials,
            RegionEndpoint regionEndpoint = null,
            AmazonSNSTransportOptions transportOptions = null,
            AmazonSimpleNotificationServiceConfig clientConfig = null)
        {
            Configure(configurer, false, defaultTopicAddress, GetTransportOptions(transportOptions, credentials, regionEndpoint, clientConfig));
        }

        /// <summary>
        /// Configures Rebus to use Amazon Simple Queue Service as the message transport
        /// </summary>
        public static void UseAmazonSNS(
            this StandardConfigurer<ITransport> configurer,
            string defaultTopicAddress,
            string accessKeyId,
            string secretAccessKey,
            RegionEndpoint regionEndpoint = null,
            AmazonSNSTransportOptions transportOptions = null,
            AmazonSimpleNotificationServiceConfig clientConfig = null)
        {
            var credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
            Configure(configurer, false, defaultTopicAddress, GetTransportOptions(transportOptions, credentials, regionEndpoint, clientConfig));
        }

        /// <summary>
        /// Configures Rebus to use Amazon Simple Queue Service as the message transport
        /// </summary>
        public static void UseAmazonSNSAsOneWayClient(
            this StandardConfigurer<ITransport> configurer,
            AWSCredentials credentials,
            RegionEndpoint regionEndpoint = null,
            AmazonSNSTransportOptions transportOptions = null,
            AmazonSimpleNotificationServiceConfig clientConfig = null)
        {
            Configure(configurer, true, null, GetTransportOptions(transportOptions, credentials, regionEndpoint, clientConfig));
        }

        /// <summary>
        /// Configures Rebus to use Amazon Simple Queue Service as the message transport
        /// </summary>
        public static void UseAmazonSNSAsOneWayClient(
            this StandardConfigurer<ITransport> configurer,
            string accessKeyId,
            string secretAccessKey,
            RegionEndpoint regionEndpoint = null,
            AmazonSNSTransportOptions transportOptions = null,
            AmazonSimpleNotificationServiceConfig clientConfig = null)
        {
            var credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
            Configure(configurer, true, null, GetTransportOptions(transportOptions, credentials, regionEndpoint, clientConfig));
        }

        internal static AmazonSNSTransportOptions GetTransportOptions(AmazonSNSTransportOptions options, AWSCredentials credentials, RegionEndpoint regionEndpoint, AmazonSimpleNotificationServiceConfig clientConfig)
        {
            options = options ?? new AmazonSNSTransportOptions();

            clientConfig = clientConfig ?? new AmazonSimpleNotificationServiceConfig();
            if (regionEndpoint != null) clientConfig.RegionEndpoint = regionEndpoint;

            if (options.ClientFactory == null)
            {
                options.ClientFactory = GetClientFactory(credentials, clientConfig);
            }
            else
            {
                if (credentials != null || clientConfig != null)
                {
                    throw new RebusConfigurationException($"Could not configure SNS client, because a client factory was provided at the same time as either AWS credentials and/or SNS config. Please EITHER provide a factory, OR provide the necessary credentials and/or config, OR do not provide anything alltogether to fall back to EC2 roles");
                }
            }

            return options;
        }

        private static Func<IAmazonSimpleNotificationService> GetClientFactory(AWSCredentials credentials, AmazonSimpleNotificationServiceConfig config)
        {
            if (credentials != null && config != null)
            {
                return () => new AmazonSimpleNotificationServiceClient(credentials, config);
            }

            if (credentials != null)
            {
                return () => new AmazonSimpleNotificationServiceClient(credentials);
            }

            if (config != null)
            {
                return () => new AmazonSimpleNotificationServiceClient(config);
            }

            return () => new AmazonSimpleNotificationServiceClient();
        }

        public static void Configure(StandardConfigurer<ITransport> configurer, bool oneWayClient, string defaultTopicAddress, AmazonSNSTransportOptions options, bool registerAsDefaultTransport = true)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (!oneWayClient && (defaultTopicAddress == null)) throw new ArgumentNullException(nameof(defaultTopicAddress));
            if (options == null) throw new ArgumentNullException(nameof(options));

            configurer
                .OtherService<AmazonSnsTransport>()
                .Register(c =>
                {
                    var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                    var asyncTaskFactory = c.Get<IAsyncTaskFactory>();
                    var rebusTime = c.Get<IRebusTime>();

                    return new AmazonSnsTransport(options, rebusLoggerFactory);
                });

            if (registerAsDefaultTransport)
            {
                configurer.Register(c => c.Get<AmazonSnsTransport>());

                configurer
                    .OtherService<ISubscriptionStorage>()
                    .Register(c => c.Get<AmazonSnsTransport>(), description: AmazonSnsSubText);

                if (oneWayClient)
                {
                    OneWayClientBackdoor.ConfigureOneWayClient(configurer);
                }
            }
        }
    }
}
