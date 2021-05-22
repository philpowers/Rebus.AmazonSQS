using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Internals;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Subscriptions;
using Rebus.Transport;

namespace Rebus.AmazonSQS
{
    /// <summary>
    /// Implementation of <see cref="ITransport"/> that uses Amazon Simple Notification Service to perform pubsub
    /// </summary>
    public class AmazonSnsTransport : AbstractRebusTransport, ISubscriptionStorage, IInitializable, IDisposable
    {
        private const string FifoTopicSuffix = ".fifo";

        public bool IsCentralized => true;

        private bool isDisposed;
        private IAmazonSimpleNotificationService client;
        private AwsAddress inputAwsAddress;

        private readonly AmazonSNSTransportOptions options;
        private readonly ILog log;

        public AmazonSnsTransport(string inputTopicAddress, AmazonSNSTransportOptions options, IRebusLoggerFactory rebusLoggerFactory) : base(inputTopicAddress)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));

            this.options = options ?? throw new ArgumentNullException(nameof(options));
            this.log = rebusLoggerFactory.GetLogger<AmazonSqsTransport>();

            if (!string.IsNullOrEmpty(inputTopicAddress))
            {
                if (!AwsAddress.TryParse(inputTopicAddress, out this.inputAwsAddress))
                {
                    var message = $"The input topic address '{inputTopicAddress}' is not valid - please either use a simple topic name (eg. 'my-topic') or the full ARN for the topic (e.g. 'arn:aws:sns:us-west-2:12345:my-topic').";
                    throw new ArgumentException(message, nameof(inputTopicAddress));
                }
            }
        }

        public void Initialize()
        {
            this.log.Info("Initializing SNS client");

            this.client = this.options.ClientFactory();

            if (this.inputAwsAddress != null)
            {
                if (this.options.CreateTopicsOptions?.CreateTopics == true)
                {
                    this.inputAwsAddress = this.CreateSnsTopic(this.inputAwsAddress);
                }

                // Make sure that we've got an ARN
                if (this.inputAwsAddress.AddressType != AwsAddressType.Arn)
                {
                    AsyncHelpers.RunSync(async () =>
                    {
                        var arnAddress = await this.LookupArnForTopic(this.inputAwsAddress.Name);
                        if (arnAddress == null)
                        {
                            throw new InvalidOperationException($"Could not find ARN for '{this.inputAwsAddress.Name}'");
                        }

                        this.inputAwsAddress = arnAddress;
                    });
                }
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public override Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task<string[]> GetSubscriberAddresses(string topic)
        {
            throw new NotImplementedException();
        }

        public Task RegisterSubscriber(string topic, string subscriberAddress)
        {
            throw new NotImplementedException();
        }

        public Task UnregisterSubscriber(string topic, string subscriberAddress)
        {
            throw new NotImplementedException();
        }

        public override void CreateQueue(string address)
        {
            this.CreateSnsTopic(AwsAddress.FromNonQualifiedName(address));
        }

        public void DeleteTopic()
        {
            // @todo(PQP): Should we try to retrieve ARN here if we don't already have it?
            if (this.inputAwsAddress?.AddressType != AwsAddressType.Arn) {
                return;
            }

            AsyncHelpers.RunSync(() => this.client.DeleteTopicAsync(this.inputAwsAddress.Name));
        }

        protected override Task SendOutgoingMessages(IEnumerable<OutgoingMessage> outgoingMessages, ITransactionContext context)
        {
            throw new NotImplementedException();
        }

        protected virtual void Dispose(bool disposing)
        {
            if (this.isDisposed)
            {
                return;
            }

            this.isDisposed = true;

            this.client?.Dispose();
        }

        private AwsAddress CreateSnsTopic(AwsAddress awsAddress)
        {
            if (this.options.CreateTopicsOptions?.CreateTopics != true)
            {
                return awsAddress;
            }

            awsAddress = null;

            AsyncHelpers.RunSync(async () =>
            {
                try
                {
                    var createTopicRequest = new CreateTopicRequest
                    {
                        Name = awsAddress.Name,

                        // See list of possible attributes here:
                        // https://docs.aws.amazon.com/sdkfornet/v3/apidocs/items/SNS/TCreateTopicRequest.html
                        Attributes = new Dictionary<string, string>
                        {
                            { "FifoTopic", this.options.CreateTopicsOptions.UseFifo.ToString() } ,
                            { "ContentBasedDeduplication", this.options.CreateTopicsOptions.ContentBasedDeduplication.ToString() }
                        }
                    };

                    var createTopicResponse = await this.client.CreateTopicAsync(awsAddress.Name);
                    if (createTopicResponse.HttpStatusCode != HttpStatusCode.OK)
                    {
                        throw new Exception($"Could not create SNS topic '{awsAddress.Name}' - got HTTP {createTopicResponse.HttpStatusCode}");
                    }

                    awsAddress = AwsAddress.FromArn(createTopicResponse.TopicArn);
                }
                catch (AmazonServiceException ex)
                {
                    throw new Exception($"Got error from AWS: {ex}");
                }
            });

            return awsAddress;
        }

        private static bool IsValidAddress(string address)
        {
            if (string.IsNullOrEmpty(address))
            {
                return false;
            }

            if (address.Contains("/")) {
                return Uri.IsWellFormedUriString(address, UriKind.Absolute);
            }

            // Assume that the address is a SNS topic name. See here for information about valid SNS topic names:
            //     https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-properties-sns-topic.html
            if (address.Length > 256)
            {
                return false;
            }

            if (address.EndsWith(FifoTopicSuffix))
            {
                // Remove FIFO suffix for topic name validation (the period in the suffix is NOT valid otherwise)
                address = address.Remove(address.Length - FifoTopicSuffix.Length);
            }

            return address.All(c => char.IsLetterOrDigit(c) || c == '_' || c == '-');
        }

        private async Task<AwsAddress> LookupArnForTopic(string topicName)
        {
            // WARNING: The implementation of FindTopicAsync loops through all existing SNS topics until it finds the
            // specified one, which could cause performance problems if there are a lot of topics associated with the
            // current AWS account.
            var topic = await this.client.FindTopicAsync(topicName);
            if (topic == null)
            {
                return null;
            }

            return AwsAddress.FromArn(topic.TopicArn);
        }

        private AwsAddress DeriveArnFromAddress(AwsAddress awsAddress)
        {
            if (awsAddress.AddressType == AwsAddressType.Arn)
            {
                return awsAddress;
            }

            // Theoretically, ARN format can change and therefore it's discouraged to generate them manually. However,
            // the format seems very stable and sometimes this is the best we can do

            throw new NotImplementedException();

            // See here for example of retrieving AWS account ID for current client: https://stackoverflow.com/a/56199669
            var accountId = "TODO";

            var arn = $"arn:aws:sns:{this.client.Config.RegionEndpoint.SystemName}:{accountId}:{awsAddress.Name}";
            return AwsAddress.FromArn(arn);
        }
    }
}
