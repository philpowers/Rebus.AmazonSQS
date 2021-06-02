namespace Rebus.Config
{
    /// <summary>
    /// Holds all of the exposed options which can be applied when using the AWSSimple transport.
    /// </summary>
    public class AmazonSimpleTransportOptions
    {
        /// <summary>
        /// Configures whether SNS topics should automatically be attached to a corresponding SQS queue. If this is
        /// enabled, SNS topic and SQS queue names will also be managed by the bus, and Rebus must be allowed to create
        /// topics and queues (ie: <see cref="AmazonSQSTransportOptions.CreateQueues" /> and <see cref="AmazonSNSTransportOptions.CreateTopicsOptions" />)
        /// Defaults to <code>false</code>.
        /// </summary>
        public bool AutoAttachServices { get; set; }

        public bool DisableAccessPolicyChecks { get; set; }

        /// <summary>
        /// Default constructor of the exposed SNS transport options.
        /// </summary>
        public AmazonSimpleTransportOptions()
        {
        }
    }
}
