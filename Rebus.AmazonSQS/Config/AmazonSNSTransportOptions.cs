using System;
using Amazon.SimpleNotificationService;
using Rebus.Bus;

namespace Rebus.Config
{
    /// <summary>
    /// Holds all of the exposed options which can be applied when using the SNS transport.
    /// </summary>
    public class AmazonSNSTransportOptions
    {
        private ushort _messageBatchSize = 10;

        /// <summary>
        /// TODO: Determine if this should be included in SNS options + check XML docs
        /// Sets the WaitTimeSeconds on the ReceiveMessage. The default setting is 1, which enables long
        /// polling for a single second. The number of seconds can be set up to 20 seconds.
        /// In case no long polling is desired, then set the value to 0.
        /// </summary>
        public int ReceiveWaitTimeSeconds { get; set; }

        /// <summary>
        /// TODO: Determine if this should be included in SNS options + check XML docs
        /// Configures whether SNS's built-in deferred messages mechanism is to be used when you <see cref="IBus.Defer"/> messages.
        /// Defaults to <code>true</code>.
        /// Please note that SNS's mechanism is only capably of deferring messages up 900 seconds, so you might need to
        /// set <see cref="UseNativeDeferredMessages"/> to <code>false</code> and then use a "real" timeout manager like e.g.
        /// one that uses SQL Server to store timeouts.
        /// </summary>
        public bool UseNativeDeferredMessages { get; set; }

        /// <summary>
        /// Sets options related to creating new topics. If set to null, Rebus expects that the topics are already created.
        /// See <see cref="AmazonSNSCreateTopicsOptions" /> for defaults.
        /// </summary>
        public AmazonSNSCreateTopicsOptions CreateTopicsOptions { get; set; }

        /// <summary>
        /// TODO: Determine if this should be included in SNS options + check XML docs
        /// Sets the MessageBatchSize for sending batch messages to SNS.
        /// Value of BatchSize can be set up to 10.
        /// Defaults to <code>10</code>.
        /// </summary>
        public ushort MessageBatchSize
        {
            get
            {
                return _messageBatchSize;
            }
            set
            {
                if (value == ushort.MinValue || value > 10)
                    throw new ArgumentOutOfRangeException(nameof(MessageBatchSize), value, "MessageBatchSize must be between 1 and 10.");

                _messageBatchSize = value;
            }
        }

        /// <summary>
        /// Default constructor of the exposed SNS transport options.
        /// </summary>
        public AmazonSNSTransportOptions()
        {
            ReceiveWaitTimeSeconds = 1;
            UseNativeDeferredMessages = true;
            CreateTopicsOptions = new AmazonSNSCreateTopicsOptions();
        }

        /// <summary>
        /// Function that gets a new instance of <see cref="IAmazonSimpleNotificationService"/>
        /// </summary>
        public Func<IAmazonSimpleNotificationService> ClientFactory;
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
