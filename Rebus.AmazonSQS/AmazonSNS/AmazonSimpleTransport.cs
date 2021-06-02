using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Auth.AccessControlPolicy;
using Amazon.Auth.AccessControlPolicy.ActionIdentifiers;
using Amazon.SQS;
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
    /// Implementation of <see cref="ITransport"/> that unifies the AWS 'Simple' services (SQS / SNS) to handle both
    /// message queueing (SQS) and pub/sub (SNS)
    /// </summary>
    public class AmazonSimpleTransport : ITransport, ISubscriptionStorage, IInitializable
    {
        public string Address => this.snsTransport?.Address;
        public bool IsCentralized => true;

        private readonly string inputQueueName;
        private readonly AmazonSnsTransport snsTransport;
        private readonly AmazonSqsTransport sqsTransport;
        private readonly AmazonSimpleTransportOptions transportOptions;
        private readonly ILog log;

        public AmazonSimpleTransport(string inputQueueAddress, AmazonSnsTransport snsTransport, AmazonSqsTransport sqsTransport, AmazonSimpleTransportOptions transportOptions, IRebusLoggerFactory rebusLoggerFactory)
        {
            this.inputQueueName = inputQueueAddress;
            this.snsTransport = snsTransport;
            this.sqsTransport = sqsTransport;
            this.transportOptions = transportOptions;

            this.log = rebusLoggerFactory.GetLogger<AmazonSimpleTransport>();
        }

        public void Initialize()
        {
            var topicName = this.GetNativeTopicName(this.inputQueueName);
            var queueAddress = this.GetNativeQueueAddress(this.inputQueueName);

            this.snsTransport.Initialize(topicName, this.transportOptions.AutoAttachServices);
            this.sqsTransport.Initialize(queueAddress, this.transportOptions.AutoAttachServices);

            if (this.transportOptions.AutoAttachServices && !string.IsNullOrEmpty(this.inputQueueName))
            {
                var queueArnAddress = AsyncHelpers.GetSync(() => this.GetSqsArnAddress(queueAddress));

                AsyncHelpers.RunSync(() => this.snsTransport.RegisterSubscriber(topicName, queueArnAddress.FullAddress));
            }

            if (this.snsTransport.DefaultTopicAwsAddress != null)
            {
                AsyncHelpers.RunSync(() => this.ValidateSubscriptionAccessPolicies(this.snsTransport.DefaultTopicAwsAddress));
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

            var snsTopicName = this.GetNativeTopicName(topic);
            await this.snsTransport.RegisterSubscriber(snsTopicName, subscriberArnAddress.FullAddress);

            if (!this.transportOptions.DisableAccessPolicyChecks)
            {
                await this.ValidateQueueAccessPolicy(subscriberArnAddress, AwsAddress.Parse(snsTopicName), true);
            }
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

        public Task<bool> CheckSqsAccessPolicy(string sqsAddress, string topic)
        {
            return this.ValidateQueueAccessPolicy(
                AwsAddress.FromNonQualifiedName(sqsAddress),
                AwsAddress.FromNonQualifiedName(topic),
                false);
        }

        private async Task ValidateSubscriptionAccessPolicies(AwsAddress snsTopicAddress)
        {
            // Validates that all SQS queues that have subscriptions to the specified SNS topic have their Access
            // Policies configured so that messages published from SNS have permissions to the queue.
            // If these access policies are not configured correctly, published messages will be SILENTLY DROPPED
            var snsSubscriptions = await this.snsTransport.ListSnsSubscriptions(snsTopicAddress);

            foreach (var subscription in snsSubscriptions)
            {
                if (subscription.Protocol != "sqs")
                {
                    continue;
                }

                if (!AwsAddress.TryParse(subscription.Endpoint, out var sqsQueueAddress))
                {
                    // NOTE: Maybe this should be considered an internal error and throw an exception due to the
                    // possibility of silently dropped messages
                    this.log.Warn($"Could not get address for SQS endpoint '{subscription.Endpoint}', skipping Access Policy check.");
                    continue;
                }

                await this.ValidateQueueAccessPolicy(sqsQueueAddress, snsTopicAddress, true);
            }
        }

        private async Task<bool> ValidateQueueAccessPolicy(AwsAddress sqsQueueAddress, AwsAddress snsTopicAddress, bool allowUpdate)
        {
            var sqsClient = this.sqsTransport.GetClient();
            if (sqsClient == null)
            {
                throw new InvalidOperationException("Could not retrieve SQS client to validate access policy!");
            }

            var sqsId = this.GetFullSqsId(sqsQueueAddress);
            var snsArnAddress = await this.snsTransport.GetSnsArnAddress(snsTopicAddress.ResourceId);

            var statement = CreateSqsPolicyStatement(sqsId.Arn, snsArnAddress.FullAddress);

            Policy updatedPolicy;

            var attributes = await sqsClient.GetAttributesAsync(sqsId.Url);
            if (attributes.TryGetValue("Policy", out var existingPolicyStr))
            {
                var sqsPolicy = Policy.FromJson(existingPolicyStr);
                if (sqsPolicy.CheckIfStatementExists(statement))
                {
                    return true;
                }

                updatedPolicy = sqsPolicy.WithStatements(statement);
            }
            else
            {
                updatedPolicy = new Policy().WithStatements(statement);
            }

            if (!allowUpdate)
            {
                return false;
            }

            this.log.Debug($"Updating SQS Access Policy of '{sqsQueueAddress.FullAddress}' to allow topic '{snsTopicAddress.FullAddress}'");
            await sqsClient.SetAttributesAsync(
                sqsId.Url,
                new Dictionary<string, string>
                {
                    { "Policy", updatedPolicy.ToJson() }
                });

            return true;
        }

        private SqsQueueIdentification GetFullSqsId(AwsAddress sqsQueueAddress)
        {
            string arn = null;
            string url = null;

            string name;
            switch (sqsQueueAddress.AddressType)
            {
                case AwsAddressType.NonQualifiedName:
                    name = sqsQueueAddress.ResourceId;
                    break;
                case AwsAddressType.Arn:
                    name = sqsQueueAddress.ResourceId;
                    arn = sqsQueueAddress.FullAddress;
                    break;
                case AwsAddressType.Url:
                    name = sqsQueueAddress.ResourceId;
                    url = sqsQueueAddress.FullAddress;
                    break;

                default:
                    throw new InvalidOperationException($"Unhandled address type '{sqsQueueAddress.AddressType}' for SQS queue address");
            }

            if (arn == null && url == null)
            {
                (url, arn) = this.sqsTransport.GetQueueId(name);
            }

            if (arn == null)
            {
                (_, arn) = this.sqsTransport.GetQueueId(name);
            }

            if (url == null)
            {
                url = this.sqsTransport.GetQueueUrlByName(name);
            }

            return new SqsQueueIdentification(name, arn, url);

        }

        private string GetNativeQueueAddress(string address)
        {
            if (string.IsNullOrEmpty(address))
            {
                return address;
            }

            return address;
        }

        private string GetNativeTopicName(string topic)
        {
            if (string.IsNullOrEmpty(topic))
            {
                return topic;
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

            if (awsAddress.AddressType == AwsAddressType.Arn)
            {
                return Task.FromResult(awsAddress);
            }

            var (_, arn) = this.sqsTransport.GetQueueId(address);
            return Task.FromResult(AwsAddress.FromArn(arn));
        }

        private static Statement CreateSqsPolicyStatement(string sqsQueueArn, string snsTopicArn)
        {
            return new Statement(Statement.StatementEffect.Allow)
                .WithPrincipals(new Principal(Principal.SERVICE_PROVIDER, "sns.amazonaws.com"))
                .WithActionIdentifiers(SQSActionIdentifiers.SendMessage)
                .WithResources(new Resource(sqsQueueArn))
                .WithConditions(ConditionFactory.NewSourceArnCondition(snsTopicArn));
        }
    }

    public class SqsQueueIdentification
    {
        public string Name { get; }
        public string Arn { get; }
        public string Url { get; }

        public SqsQueueIdentification(string name, string arn, string url)
        {
            this.Name = name;
            this.Arn = arn;
            this.Url = url;
        }
    }
}
