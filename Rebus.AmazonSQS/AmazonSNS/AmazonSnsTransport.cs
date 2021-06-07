using System;
using System.Collections.Concurrent;
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
using Rebus.Exceptions;
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

        internal AwsAddress DefaultTopicAwsAddress { get; private set; }

        private bool isDisposed;
        private IAmazonSimpleNotificationService client;
        private bool warnedOnArnLookup;

        readonly AmazonTransportMessageSerializer serializer = new AmazonTransportMessageSerializer();

        private readonly ConcurrentDictionary<string, string> topicArnCache;
        private readonly AmazonSNSTransportOptions options;
        private readonly ILog log;


        public AmazonSnsTransport(string defaultTopicAddress, AmazonSNSTransportOptions options, IRebusLoggerFactory rebusLoggerFactory) : base(null)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));

            this.options = options ?? throw new ArgumentNullException(nameof(options));
            this.log = rebusLoggerFactory.GetLogger<AmazonSqsTransport>();

            if (!string.IsNullOrEmpty(defaultTopicAddress))
            {
                if (!AwsAddress.TryParse(defaultTopicAddress, out var defaultTopicAwsAddress))
                {
                    var message = $"The default topic address '{defaultTopicAddress}' is not valid - please use either the full ARN for the topic (e.g. 'arn:aws:sns:us-west-2:12345:my-topic') or a simple topic name (eg. 'my-topic').";
                    throw new ArgumentException(message, nameof(defaultTopicAddress));
                }

                this.DefaultTopicAwsAddress = defaultTopicAwsAddress;
            }

            this.topicArnCache = new ConcurrentDictionary<string, string>();
        }

        public void Initialize()
        {
            this.log.Info("Initializing SNS client");

            this.client = this.options.ClientFactory();

            if (this.DefaultTopicAwsAddress != null)
            {
                AsyncHelpers.RunSync(async () =>
                {
                    if (this.options.CreateTopicsOptions?.CreateTopics == true)
                    {
                        this.DefaultTopicAwsAddress = await this.CreateSnsTopic(this.DefaultTopicAwsAddress.ResourceId);
                    }

                    // Make sure that we've got an ARN
                    if (this.DefaultTopicAwsAddress.AddressType != AwsAddressType.Arn)
                    {
                        var topicArn = await this.LookupArnForTopicName(this.DefaultTopicAwsAddress.ResourceId);
                        if (topicArn == null)
                        {
                            throw new InvalidOperationException($"Could not find ARN for '{this.DefaultTopicAwsAddress.ResourceId}'");
                        }

                        this.DefaultTopicAwsAddress = AwsAddress.FromArn(topicArn);
                    }
                });

                // Seed the topic -> ARN cache with our own topic name
                this.topicArnCache[this.DefaultTopicAwsAddress.ResourceId] = this.DefaultTopicAwsAddress.FullAddress;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public override Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            throw new InvalidOperationException("SNS does not support directly receiving (pulling) messages");
        }

        /// <summary>
        /// Gets "subscriber addresses" as one single magic "queue address", which will be interpreted as a proper
        /// pub/sub topic when the time comes to send to it. This is the method used by Rebus for messaging purposes.
        /// To get the actual list of raw SNS subscriptions (eg, for testing purposes), use <see cref="ListSnsSubscriptions(string)" />
        /// instead
        /// </summary>
        public async Task<string[]> GetSubscriberAddresses(string topic)
        {
            var internalTopicName = GetInternalTopicName(topic);

            var arnAddress = await this.GetSnsArnAddress(internalTopicName, false);
            if (arnAddress == null)
            {
                throw new InvalidOperationException($"{nameof(GetSubscriberAddresses)} could not get ARN for topic '{internalTopicName}'");
            }

            return new string[] { arnAddress.FullAddress };
        }

        public async Task RegisterSubscriber(string topic, string subscriberAddress)
        {
            var internalTopicName = GetInternalTopicName(topic);

            var topicArnAddress = await this.GetSnsArnAddress(internalTopicName, true);
            if (topicArnAddress == null)
            {
                throw new InvalidOperationException($"{nameof(RegisterSubscriber)} could not get ARN for topic '{internalTopicName}'");
            }

            if (!AwsAddress.TryParse(subscriberAddress, out var subscriberArnAddress) || (subscriberArnAddress.AddressType != AwsAddressType.Arn))
            {
                throw new ArgumentException($"{nameof(RegisterSubscriber)} subscriber address '{subscriberAddress}' must be a valid ARN ", nameof(subscriberAddress));
            }

            if (subscriberArnAddress.ServiceType != AwsServiceType.Sqs)
            {
                throw new InvalidOperationException($"{nameof(RegisterSubscriber)} unsupported subscriber address '{subscriberAddress}'; the SNS transport currently only supports SQS subscribers");
            }

            var subscriptionResponse = await this.client.SubscribeAsync(
                topicArnAddress.FullAddress,
                "sqs",
                subscriberArnAddress.FullAddress);

            if (subscriptionResponse.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new RebusApplicationException(
                    $"Subscription of SNS topic '{topicArnAddress}' to '{subscriberArnAddress}' failed. HTTP status code: {subscriptionResponse.HttpStatusCode}; Request ID: {subscriptionResponse.ResponseMetadata?.RequestId}");
            }

            await this.client.SetSubscriptionAttributesAsync(subscriptionResponse.SubscriptionArn, "RawMessageDelivery", "True");

            log.Info($"SNS topic '{topicArnAddress}' subscribed to SQS queue '{subscriberArnAddress}' with subscription ARN '{subscriptionResponse.SubscriptionArn}'");
        }

        public async Task UnregisterSubscriber(string topic, string subscriberAddress)
        {
            var internalTopicName = GetInternalTopicName(topic);

            var topicArnAddress = await this.GetSnsArnAddress(internalTopicName, false);
            if (topicArnAddress == null)
            {
                throw new InvalidOperationException($"{nameof(UnregisterSubscriber)} could not get ARN for topic '{internalTopicName}'");
            }

            var subscriberArnAddress = await this.GetSnsArnAddress(subscriberAddress, false);
            if (subscriberArnAddress == null)
            {
                throw new InvalidOperationException($"{nameof(UnregisterSubscriber)} could not parse subscriber address '{subscriberAddress}'");
            }

            if (subscriberArnAddress.ServiceType != AwsServiceType.Sqs)
            {
                throw new InvalidOperationException($"{nameof(UnregisterSubscriber)} unsupported subscriber address '{subscriberAddress}'; the SNS transport currently only supports SQS subscribers");
            }


            var snsSubscriptions = await this.ListSnsSubscriptions(topicArnAddress);
            var targetSubArn = snsSubscriptions.FirstOrDefault(s => s.Endpoint == subscriberArnAddress.FullAddress)?.SubscriptionArn;
            if (targetSubArn == null)
            {
                this.log.Info($"Could not find subscription ARN for topic '{topicArnAddress}' and subscriber '{subscriberArnAddress}', ignoring.");
                return;
            }

            var unsubscriberResponse = await this.client.UnsubscribeAsync(targetSubArn);

            if (unsubscriberResponse.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new RebusApplicationException(
                    $"Unsubscribe of SNS topic '{topicArnAddress}' from '{subscriberArnAddress}' failed. HTTP status code: {unsubscriberResponse.HttpStatusCode}; Request ID: {unsubscriberResponse.ResponseMetadata?.RequestId}");
            }
        }

        public override void CreateQueue(string address)
        {
            AsyncHelpers.RunSync(() => this.CreateSnsTopic(address));
        }

        public void DeleteTopic()
        {
            // @todo(PQP): Should we try to retrieve ARN here if we don't already have it?
            if (this.DefaultTopicAwsAddress?.AddressType != AwsAddressType.Arn) {
                return;
            }

            AsyncHelpers.RunSync(() => this.client.DeleteTopicAsync(this.DefaultTopicAwsAddress.FullAddress));
        }

        public async Task<string> LookupArnForTopicName(string topicName)
        {
            var internalTopicName = GetInternalTopicName(topicName);

            return this.topicArnCache.GetOrAdd(internalTopicName, _ =>
            {
                // WARNING: The implementation of FindTopicAsync loops through all existing SNS topics until it finds
                // the specified one, which could cause performance problems if there are a lot of topics associated
                // with the current AWS account.
                var topic = AsyncHelpers.GetSync(() => this.client.FindTopicAsync(internalTopicName));
                return topic?.TopicArn;
            });
        }

        public async Task<List<Subscription>> ListSnsSubscriptions(string topic)
        {
            var internalTopicName = GetInternalTopicName(topic);

            var topicAddress = await this.GetSnsArnAddress(internalTopicName, false);
            if (topicAddress == null)
            {
                throw new InvalidOperationException($"Could not get address for topic '{internalTopicName}'");
            }

            return await this.ListSnsSubscriptions(topicAddress);
        }

        internal async Task<List<Subscription>> ListSnsSubscriptions(AwsAddress topicAddress)
        {
            if (topicAddress.AddressType != AwsAddressType.Arn)
            {
                throw new InvalidOperationException($"Topic address is in invalid format '{topicAddress.AddressType}'");
            }

            var subsPaginator = this.client.Paginators.ListSubscriptionsByTopic(
                new ListSubscriptionsByTopicRequest(topicAddress.FullAddress));

            return await subsPaginator.Subscriptions.ToListAsync();
        }

        internal async Task<AwsAddress> GetSnsArnAddress(string internalTopicName, bool createIfNotExists)
        {
            if (string.IsNullOrEmpty(internalTopicName))
            {
                return null;
            }

            if (!AwsAddress.TryParse(internalTopicName, out var awsAddress))
            {
                return null;
            }

            if (awsAddress.AddressType == AwsAddressType.Arn)
            {
                return awsAddress;
            }

            if (!this.warnedOnArnLookup)
            {
                this.warnedOnArnLookup = true;
                this.log.Info($"Getting ARN for topic '{internalTopicName}'; for best performance, specify all SNS addresses using their ARNs");
            }

            var arnStr = await this.LookupArnForTopicName(internalTopicName);
            if (!string.IsNullOrEmpty(arnStr))
            {
                return AwsAddress.FromArn(arnStr);
            }

            if (!createIfNotExists)
            {
                return null;
            }

            awsAddress = await this.CreateSnsTopic(internalTopicName);
            if (awsAddress != null)
            {
                this.topicArnCache[internalTopicName] = awsAddress.FullAddress;
            }

            return awsAddress;
        }

        protected override async Task SendOutgoingMessages(IEnumerable<OutgoingMessage> outgoingMessages, ITransactionContext context)
        {
            foreach (var outgoingMessage in outgoingMessages)
            {
                var internalTopicName = GetInternalTopicName(outgoingMessage.DestinationAddress);

                var destinationArnAddress = await this.GetSnsArnAddress(internalTopicName, true);
                if (destinationArnAddress == null)
                {
                    throw new RebusApplicationException($"Could not find ARN for destination SNS topic: {internalTopicName}");
                }

                // @todo(PQP): Implement support for setting Subject through headers
                var subject = (string)null;

                var snsMessage = new AmazonTransportMessage(
                    outgoingMessage.TransportMessage.Headers,
                    Convert.ToBase64String(outgoingMessage.TransportMessage.Body));

                var messageBody = this.serializer.Serialize(snsMessage);

                var publishResponse = (subject != null)
                    ? await this.client.PublishAsync(destinationArnAddress.FullAddress, subject, messageBody)
                    : await this.client.PublishAsync(destinationArnAddress.FullAddress, messageBody);

                if (publishResponse.HttpStatusCode != HttpStatusCode.OK)
                {
                    throw new RebusApplicationException(
                        $"Publish to SNS topic '{destinationArnAddress.FullAddress}' failed. HTTP status code: {publishResponse.HttpStatusCode}; Request ID: {publishResponse.ResponseMetadata?.RequestId}");
                }
            }
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

        private async Task<AwsAddress> CreateSnsTopic(string internalTopicName)
        {
            if (this.options.CreateTopicsOptions?.CreateTopics != true)
            {
                return null;
            }

            this.log.Info("Creating topic {topicName}", internalTopicName);

            try
            {
                var createTopicRequest = new CreateTopicRequest
                {
                    Name = internalTopicName,

                    // See list of possible attributes here:
                    // https://docs.aws.amazon.com/sdkfornet/v3/apidocs/items/SNS/TCreateTopicRequest.html
                    Attributes = new Dictionary<string, string>
                    {
                        { "FifoTopic", this.options.CreateTopicsOptions.UseFifo.ToString() } ,
                        { "ContentBasedDeduplication", this.options.CreateTopicsOptions.ContentBasedDeduplication.ToString() }
                    }
                };

                var createTopicResponse = await this.client.CreateTopicAsync(internalTopicName);
                if (createTopicResponse.HttpStatusCode != HttpStatusCode.OK)
                {
                    throw new Exception($"Could not create SNS topic '{internalTopicName}' - got HTTP {createTopicResponse.HttpStatusCode}");
                }

                return AwsAddress.FromArn(createTopicResponse.TopicArn);
            }
            catch (AmazonServiceException ex)
            {
                throw new Exception($"Got error from AWS: {ex}");
            }
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

            var arn = $"arn:aws:sns:{this.client.Config.RegionEndpoint.SystemName}:{accountId}:{awsAddress.ResourceId}";
            return AwsAddress.FromArn(arn);
        }

        private static bool IsValidInternalCharacter(char c)
        {
            // See: https://docs.aws.amazon.com/sns/latest/api/API_CreateTopic.html
            return char.IsLetterOrDigit(c) || c == '_' || c == '-';
        }

        private static string GetInternalTopicName(string topic)
        {
            if (topic.StartsWith("arn:aws:sns:"))
            {
                return topic;
            }

            return string.Concat(topic.Select(c => IsValidInternalCharacter(c) ? c : '_'));
        }
    }
}
