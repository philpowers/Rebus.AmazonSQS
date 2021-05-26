using System;
using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Rebus.AmazonSQS;
using Rebus.Exceptions;
using Rebus.Logging;
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
            if (clientConfig == null) clientConfig = new AmazonSimpleNotificationServiceConfig();
            if (regionEndpoint != null) clientConfig.RegionEndpoint = regionEndpoint;

            Configure(configurer, defaultTopicAddress, GetTransportOptions(transportOptions, credentials, clientConfig));
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
            if (clientConfig == null) clientConfig = new AmazonSimpleNotificationServiceConfig();
            if (regionEndpoint != null) clientConfig.RegionEndpoint = regionEndpoint;

            var credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);

            Configure(configurer, defaultTopicAddress, GetTransportOptions(transportOptions, credentials, clientConfig));
        }

        /// <summary>
        /// Configures Rebus to use Amazon Simple Queue Service as the message transport
        /// </summary>
        public static void UseAmazonSNSAsOneWayClient(
            this StandardConfigurer<ITransport> configurer,
            AWSCredentials credentials,
            RegionEndpoint regionEndpoint = null,
            AmazonSNSTransportOptions options = null,
            AmazonSimpleNotificationServiceConfig clientConfig = null)
        {
            if (clientConfig == null) clientConfig = new AmazonSimpleNotificationServiceConfig();
            if (regionEndpoint != null) clientConfig.RegionEndpoint = regionEndpoint;

            ConfigureOneWayClient(configurer, GetTransportOptions(options, credentials, clientConfig));
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
            if (clientConfig == null) clientConfig = new AmazonSimpleNotificationServiceConfig();
            if (regionEndpoint != null) clientConfig.RegionEndpoint = regionEndpoint;

            var credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);

            ConfigureOneWayClient(configurer, GetTransportOptions(transportOptions, credentials, clientConfig));
        }

        private static AmazonSNSTransportOptions GetTransportOptions(AmazonSNSTransportOptions options, AWSCredentials credentials, AmazonSimpleNotificationServiceConfig clientConfig)
        {
            options = options ?? new AmazonSNSTransportOptions();

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

        private static void Configure(StandardConfigurer<ITransport> configurer, string defaultTopicAddress, AmazonSNSTransportOptions options)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (defaultTopicAddress == null) throw new ArgumentNullException(nameof(defaultTopicAddress));
            if (options == null) throw new ArgumentNullException(nameof(options));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var asyncTaskFactory = c.Get<IAsyncTaskFactory>();
                var rebusTime = c.Get<IRebusTime>();

                return new AmazonSnsTransport(defaultTopicAddress, options, rebusLoggerFactory);
            });
        }

        private static void ConfigureOneWayClient(StandardConfigurer<ITransport> configurer, AmazonSNSTransportOptions options)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (options == null) throw new ArgumentNullException(nameof(options));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var asyncTaskFactory = c.Get<IAsyncTaskFactory>();
                var rebusTime = c.Get<IRebusTime>();

                return new AmazonSnsTransport(null, options, rebusLoggerFactory);
            });

            OneWayClientBackdoor.ConfigureOneWayClient(configurer);
        }
    }
}
