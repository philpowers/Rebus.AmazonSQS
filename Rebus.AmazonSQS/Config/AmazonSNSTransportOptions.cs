using Amazon.SimpleNotificationService;

namespace Rebus.Config
{
    /// <summary>
    /// Holds all of the exposed options which can be applied when using the SNS transport.
    /// </summary>
    public class AmazonSNSTransportOptions : AmazonTransportOptions<IAmazonSimpleNotificationService>
    {
        /// <summary>
        /// Sets options related to creating new topics. If set to null, Rebus expects that the topics are already created.
        /// See <see cref="AmazonSNSCreateTopicsOptions" /> for defaults.
        /// </summary>
        public AmazonSNSCreateTopicsOptions CreateTopicsOptions { get; set; }

        /// <summary>
        /// Default constructor of the exposed SNS transport options.
        /// </summary>
        public AmazonSNSTransportOptions()
        {
            CreateTopicsOptions = new AmazonSNSCreateTopicsOptions();
        }
    }

    public class AmazonSNSCreateTopicsOptions
    {
        /// <summary>
        /// Configures whether Rebus is in control to create topics or not. If set to false, Rebus expects that the
        /// topics are already created.
        /// Defaults to <code>true</code>.
        /// </summary>
        public bool CreateTopics { get; set; }

        /// <summary>
        /// When true, new topics are created as FIFO topics. General information about SNS FIFO topics can be found at
        /// <see href="https://docs.aws.amazon.com/sns/latest/dg/sns-fifo-topics.html" />
        /// Defaults to <code>false</code>.
        /// </summary>
        public bool UseFifo { get; set; }

        /// <summary>
        /// Only valid when UseFifo is true. This enables internal support with AWS SNS to detect duplicate messages
        /// based on the message content. If using FIFO topics and this is false, published messages must contain a
        /// MessageDeduplicationId field (<see href="https://docs.aws.amazon.com/sns/latest/api/API_Publish.html" />).
        /// Defaults to <code>false</code>.
        /// </summary>
        public bool ContentBasedDeduplication { get; set; }

        public AmazonSNSCreateTopicsOptions()
        {
            this.CreateTopics = true;
            this.UseFifo = false;
            this.ContentBasedDeduplication = false;
        }
    }
}
