using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
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

        private bool isDisposed;
        private IAmazonSimpleNotificationService client;
        private AwsAddress inputAwsAddress;
        private bool warnedOnArnLookup;

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
                    var message = $"The input topic address '{inputTopicAddress}' is not valid - please use either the full ARN for the topic (e.g. 'arn:aws:sns:us-west-2:12345:my-topic') or a simple topic name (eg. 'my-topic').";
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
                        var topicArn = await this.LookupArnForTopicName(this.inputAwsAddress.ResourceId);
                        if (topicArn == null)
                        {
                            throw new InvalidOperationException($"Could not find ARN for '{this.inputAwsAddress.ResourceId}'");
                        }

                        this.inputAwsAddress = AwsAddress.FromArn(topicArn);
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
            throw new InvalidOperationException("SNS does not support directly receiving messages");
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

            AsyncHelpers.RunSync(() => this.client.DeleteTopicAsync(this.inputAwsAddress.FullAddress));
        }

        public async Task<string> LookupArnForTopicName(string topicName)
        {
            // WARNING: The implementation of FindTopicAsync loops through all existing SNS topics until it finds the
            // specified one, which could cause performance problems if there are a lot of topics associated with the
            // current AWS account.
            var topic = await this.client.FindTopicAsync(topicName);
            return topic?.TopicArn;
        }

        protected override async Task SendOutgoingMessages(IEnumerable<OutgoingMessage> outgoingMessages, ITransactionContext context)
        {
            foreach (var outgoingMessage in outgoingMessages)
            {
                var destinationArnAddress = await this.GetArnAddress(outgoingMessage.DestinationAddress);
                if (destinationArnAddress == null)
                {
                    throw new RebusApplicationException($"Could not find ARN for destination SNS topic: {outgoingMessage.DestinationAddress}");
                }

                var subject = (string)null;

                if (!outgoingMessage.TransportMessage.Headers[Headers.ContentType].Contains("json"))
                {
                    throw new InvalidOperationException($"SNS messages must be encoded in JSON");
                }

                var messageJsonText = Encoding.UTF8.GetString(outgoingMessage.TransportMessage.Body);

                var publishResponse = (subject != null)
                    ? await this.client.PublishAsync(destinationArnAddress.FullAddress, subject, messageJsonText)
                    : await this.client.PublishAsync(destinationArnAddress.FullAddress, messageJsonText);

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

        private AwsAddress CreateSnsTopic(AwsAddress awsAddress)
        {
            if (this.options.CreateTopicsOptions?.CreateTopics != true)
            {
                return awsAddress;
            }

            AwsAddress awsAddressFromResponse = null;

            AsyncHelpers.RunSync(async () =>
            {
                try
                {
                    var createTopicRequest = new CreateTopicRequest
                    {
                        Name = awsAddress.ResourceId,

                        // See list of possible attributes here:
                        // https://docs.aws.amazon.com/sdkfornet/v3/apidocs/items/SNS/TCreateTopicRequest.html
                        Attributes = new Dictionary<string, string>
                        {
                            { "FifoTopic", this.options.CreateTopicsOptions.UseFifo.ToString() } ,
                            { "ContentBasedDeduplication", this.options.CreateTopicsOptions.ContentBasedDeduplication.ToString() }
                        }
                    };

                    var createTopicResponse = await this.client.CreateTopicAsync(awsAddress.ResourceId);
                    if (createTopicResponse.HttpStatusCode != HttpStatusCode.OK)
                    {
                        throw new Exception($"Could not create SNS topic '{awsAddress.ResourceId}' - got HTTP {createTopicResponse.HttpStatusCode}");
                    }

                    awsAddressFromResponse = AwsAddress.FromArn(createTopicResponse.TopicArn);
                }
                catch (AmazonServiceException ex)
                {
                    throw new Exception($"Got error from AWS: {ex}");
                }
            });

            return awsAddressFromResponse;
        }

        private async Task<AwsAddress> GetArnAddress(string address)
        {
            if (string.IsNullOrEmpty(address))
            {
                return null;
            }

            if (!AwsAddress.TryParse(address, out var awsAddress))
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
                this.log.Warn($"Getting ARN for topic '{address}'; for best performance, specify all SNS addresses using their ARNs");
            }

            var arnStr = await this.LookupArnForTopicName(address);
            if (string.IsNullOrEmpty(arnStr))
            {
                return null;
            }

            return AwsAddress.FromArn(arnStr);
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
    }
}
