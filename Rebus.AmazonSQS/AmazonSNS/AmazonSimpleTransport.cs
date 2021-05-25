using System;
using System.Threading;
using System.Threading.Tasks;

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
    public class AmazonSimpleTransport : ITransport, ISubscriptionStorage
    {
        public string Address => this.SqsTransport.Address;
        public bool IsCentralized => this.SnsTransport.IsCentralized;

        public AmazonSnsTransport SnsTransport { get; }
        public AmazonSqsTransport SqsTransport { get; }

        public AmazonSimpleTransport(AmazonSnsTransport snsTransport, AmazonSqsTransport sqsTransport)
        {
            this.SnsTransport = snsTransport;
            this.SqsTransport = sqsTransport;
        }

        public void CreateQueue(string address)
        {
            throw new NotSupportedException($"{nameof(CreateQueue)} is not supported. Use the SnsTransport and SqsTransport directly instead.");
        }

        public Task<string[]> GetSubscriberAddresses(string topic)
        {
            return this.SnsTransport.GetSubscriberAddresses(topic);
        }

        public async Task RegisterSubscriber(string topic, string subscriberAddress)
        {
            var subscriberArnAddress = await this.GetSqsArnAddress(subscriberAddress);
            if (subscriberArnAddress == null)
            {
                throw new ArgumentException($"{nameof(RegisterSubscriber)} could not retrieve ARN for subscriber '{subscriberAddress}'", subscriberAddress);
            }

            await this.SnsTransport.RegisterSubscriber(topic, subscriberArnAddress.FullAddress);
        }

        public async Task UnregisterSubscriber(string topic, string subscriberAddress)
        {
            var subscriberArnAddress = await this.GetSqsArnAddress(subscriberAddress);
            if (subscriberArnAddress == null)
            {
                throw new ArgumentException($"{nameof(UnregisterSubscriber)} could not retrieve ARN for subscriber '{subscriberAddress}'", subscriberAddress);
            }

            await this.SnsTransport.UnregisterSubscriber(topic, subscriberAddress);
        }

        public Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            return this.SqsTransport.Receive(context, cancellationToken);
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
                    return this.SqsTransport.Send(destinationAddress, message, context);

                case AwsServiceType.Sns:
                case AwsServiceType.Unknown:
                    return this.SnsTransport.Send(destinationAddress, message, context);

                default:
                    throw new InvalidOperationException($"Unsupported service '{destAwsAddress.ServiceType}' for destination address '{destinationAddress}'");
            }
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
                : AwsAddress.FromArn(this.SqsTransport.GetQueueArn(address));

            return Task.FromResult(sqsArn);
        }
    }
}
