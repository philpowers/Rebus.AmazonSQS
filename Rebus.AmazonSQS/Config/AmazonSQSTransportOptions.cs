﻿using System;
using Amazon.SQS;
using Rebus.Bus;

namespace Rebus.Config
{
    /// <summary>
    /// Holds all of the exposed options which can be applied when using the SQS transport.
    /// </summary>
    public class AmazonSQSTransportOptions : AmazonTransportOptions<IAmazonSQS>
    {
        private ushort _messageBatchSize = 10;

        /// <summary>
        /// Sets the WaitTimeSeconds on the ReceiveMessage. The default setting is 1, which enables long
        /// polling for a single second. The number of seconds can be set up to 20 seconds.
        /// In case no long polling is desired, then set the value to 0.
        /// </summary>
        public int ReceiveWaitTimeSeconds { get; set; }

        /// <summary>
        /// Configures whether SQS's built-in deferred messages mechanism is to be used when you <see cref="IBus.Defer"/> messages.
        /// Defaults to <code>true</code>.
        /// Please note that SQS's mechanism is only capably of deferring messages up 900 seconds, so you might need to
        /// set <see cref="UseNativeDeferredMessages"/> to <code>false</code> and then use a "real" timeout manager like e.g.
        /// one that uses SQL Server to store timeouts.
        /// </summary>
        public bool UseNativeDeferredMessages { get; set; }

        /// <summary>
        /// Configures whether Rebus is in control to create queues or not. If set to false, Rebus expects that the queue's are already created.
        /// Defaults to <code>true</code>.
        /// </summary>
        public bool CreateQueues { get; set; }

        /// <summary>
        /// Sets the MessageBatchSize for sending batch messages to SQS.
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
        /// Default constructor of the exposed SQS transport options.
        /// </summary>
        public AmazonSQSTransportOptions()
        {
            ReceiveWaitTimeSeconds = 1;
            UseNativeDeferredMessages = true;
            CreateQueues = true;
        }
    }
}
