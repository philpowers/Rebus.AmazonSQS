using System;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Internals;
using Rebus.Messages;
using Rebus.Subscriptions;
using Rebus.Transport;

namespace Rebus.AmazonSQS
{
    /// <summary>
    /// Implementation of <see cref="ITransport"/> that unifies the AWS 'Simple' services (SQS / SNS) to handle both
    /// message queueing (SQS) and pub/sub (SNS)
    /// </summary>
    public class AmazonSimpleTransport : ITransport, ISubscriptionStorage, IInitializable
    {
        private const string AutoAttachedTopicSuffix = "-rebustopic";
        private const string AutoAttachedQueueSuffix = "-rebusqueue";

        public string Address => this.snsTransport?.Address;
        public bool IsCentralized => true;

        private readonly string inputQueueName;
        private readonly AmazonSnsTransport snsTransport;
        private readonly AmazonSqsTransport sqsTransport;
        private readonly AmazonSimpleTransportOptions transportOptions;

        public AmazonSimpleTransport(string inputQueueAddress, AmazonSnsTransport snsTransport, AmazonSqsTransport sqsTransport, AmazonSimpleTransportOptions transportOptions)
        {
            this.inputQueueName = inputQueueAddress;
            this.snsTransport = snsTransport;
            this.sqsTransport = sqsTransport;
            this.transportOptions = transportOptions;
        }

        public void Initialize()
        {
            var topicName = this.GetNativeTopicName(this.inputQueueName);
            var queueAddress = this.GetNativeQueueAddress(this.inputQueueName);

            this.snsTransport.Initialize(topicName, true);
            this.sqsTransport.Initialize(queueAddress, true);

            if (this.transportOptions.AutoAttachServices && !string.IsNullOrEmpty(this.inputQueueName))
            {
                // this.snsTransport.CreateQueue(topicName);
                // this.sqsTransport.CreateQueue(queueName);

                var queueArnAddress = AsyncHelpers.GetSync(() => this.GetSqsArnAddress(queueAddress));

                AsyncHelpers.RunSync(() => this.snsTransport.RegisterSubscriber(topicName, queueArnAddress.FullAddress));
            }
        }

        public void CreateQueue(string address)
        {
            if (!this.transportOptions.AutoAttachServices)
            {
                throw new NotSupportedException($"{nameof(CreateQueue)} is not supported. Use the snsTransport and sqsTransport directly instead.");
            }

            this.snsTransport.CreateQueue(this.GetNativeTopicName(address));
            this.sqsTransport.CreateQueue(this.GetNativeQueueAddress(address));
        }

        public Task<string[]> GetSubscriberAddresses(string topic)
        {
            topic = this.GetNativeTopicName(topic);
            return this.snsTransport.GetSubscriberAddresses(topic);
        }

        public async Task RegisterSubscriber(string topic, string subscriberAddress)
        {
            subscriberAddress = this.GetNativeQueueAddress(subscriberAddress);

            var subscriberArnAddress = await this.GetSqsArnAddress(subscriberAddress);
            if (subscriberArnAddress == null)
            {
                throw new ArgumentException($"{nameof(RegisterSubscriber)} could not retrieve ARN for subscriber '{subscriberAddress}'", subscriberAddress);
            }

            await this.snsTransport.RegisterSubscriber(this.GetNativeTopicName(topic), subscriberArnAddress.FullAddress);
        }

        public async Task UnregisterSubscriber(string topic, string subscriberAddress)
        {
            subscriberAddress = this.GetNativeQueueAddress(subscriberAddress);

            var subscriberArnAddress = await this.GetSqsArnAddress(subscriberAddress);
            if (subscriberArnAddress == null)
            {
                throw new ArgumentException($"{nameof(UnregisterSubscriber)} could not retrieve ARN for subscriber '{subscriberAddress}'", subscriberAddress);
            }

            await this.snsTransport.UnregisterSubscriber(topic, subscriberAddress);
        }

        public Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            return this.sqsTransport.Receive(context, cancellationToken);
        }

        public Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            if (!AwsAddress.TryParse(destinationAddress, out var destAwsAddress))
            {
                throw new InvalidOperationException($"Could not parse destination address '{destinationAddress}'");
            }

            switch (destAwsAddress.ServiceType)
            {
                case AwsServiceType.Sqs:
                    return this.sqsTransport.Send(this.GetNativeQueueAddress(destinationAddress), message, context);

                case AwsServiceType.Sns:
                case AwsServiceType.Unknown:
                    return this.snsTransport.Send(this.GetNativeTopicName(destinationAddress), message, context);

                default:
                    throw new InvalidOperationException($"Unsupported service '{destAwsAddress.ServiceType}' for destination address '{destinationAddress}'");
            }
        }

        private string GetNativeQueueAddress(string address)
        {
            if (string.IsNullOrEmpty(address))
            {
                return address;
            }

            if (this.transportOptions.AutoAttachServices)
            {
                address += AutoAttachedQueueSuffix;
            }
            return address;
        }

        private string GetNativeTopicName(string topic)
        {
            if (string.IsNullOrEmpty(topic))
            {
                return topic;
            }

            if (this.transportOptions.AutoAttachServices)
            {
                topic += AutoAttachedTopicSuffix;
            }
            return topic;
        }

        private Task<AwsAddress> GetSqsArnAddress(string address)
        {
            if (string.IsNullOrEmpty(address))
            {
                return null;
            }

            if (!AwsAddress.TryParse(address, out var awsAddress))
            {
                return null;
            }

            var sqsArn = (awsAddress.AddressType == AwsAddressType.Arn)
                ? awsAddress
                : AwsAddress.FromArn(this.sqsTransport.GetQueueArn(address));

            return Task.FromResult(sqsArn);
        }
    }
}
