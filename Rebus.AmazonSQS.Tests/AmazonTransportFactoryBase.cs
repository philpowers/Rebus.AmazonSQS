using System;
using System.IO;
using System.Collections.Concurrent;
using System.Collections.Generic;

using Rebus.Transport;
using Rebus.Extensions;
using Rebus.Exceptions;
using Rebus.Tests.Contracts.Transports;
using System.Threading.Tasks;
using Rebus.Messages;
using System.Threading;
using Rebus.Subscriptions;

namespace Rebus.AmazonSQS.Tests
{
    public abstract class AmazonTransportFactoryBase<TTransportOptions> : ITransportFactory where TTransportOptions : class, new()
    {
        private readonly ConcurrentStack<IDisposable> _disposables = new ConcurrentStack<IDisposable>();
        private readonly Dictionary<string, ITransport> _queuesToDelete = new Dictionary<string, ITransport>();
        private readonly Action<ITransport> fnDeleteQueue;
        private readonly string queuePrefix;

        protected AmazonTransportFactoryBase(Action<ITransport> fnDeleteQueue, string queuePrefix)
        {
            this.fnDeleteQueue = fnDeleteQueue;
            this.queuePrefix = $"{queuePrefix}-{Environment.TickCount}-";
        }

        public ITransport Create(string inputQueueAddress, TimeSpan peeklockDuration, TTransportOptions options = null)
        {
            if (inputQueueAddress == null)
            {
                // one-way client
                var transport = CreateInstance(null, peeklockDuration, options);
                if (transport is IDisposable disposable)
                {
                    _disposables.Push(disposable);
                }

                return transport;
                return new AmazonTransportPrefixDecorator(transport, this.queuePrefix);
            }

            return _queuesToDelete.GetOrAdd(inputQueueAddress, () =>
            {
                var transport = CreateInstance(inputQueueAddress, peeklockDuration, options);
                if (transport is IDisposable disposable)
                {
                    _disposables.Push(disposable);
                }

                return transport;
                return new AmazonTransportPrefixDecorator(transport, this.queuePrefix);
            });
        }

        public void CleanUp()
        {
            CleanUp(false);
        }

        public void CleanUp(bool deleteQueues)
        {
            if (deleteQueues)
            {
                foreach (var queueAndTransport in _queuesToDelete)
                {
                    var transport = queueAndTransport.Value;
                    this.fnDeleteQueue(transport);
                }
            }

            while (_disposables.TryPop(out var disposable))
            {
                Console.WriteLine($"Disposing {disposable}");
                disposable.Dispose();
            }
        }

        public ITransport CreateOneWayClient()
        {
            return Create(null, TimeSpan.FromSeconds(30));
        }

        public ITransport Create(string inputQueueAddress)
        {
            return Create(inputQueueAddress, TimeSpan.FromSeconds(30));
        }

        protected abstract ITransport CreateInstance(string inputQueueAddress, TimeSpan peeklockDuration, TTransportOptions options);

        protected static string GetFilePath(string fileName)
        {
            var baseDirectory = AppContext.BaseDirectory;

            // added because of test run issues on MacOS
            var indexOfBin = baseDirectory.LastIndexOf("bin", StringComparison.OrdinalIgnoreCase);
            var connectionStringFileDirectory = baseDirectory.Substring(0, (indexOfBin > 0) ? indexOfBin : baseDirectory.Length);
            return Path.Combine(connectionStringFileDirectory, fileName);
        }

        protected static ConnectionInfo ConnectionInfoFromEnvironmentVariable(string environmentVariableName)
        {
            var value = Environment.GetEnvironmentVariable(environmentVariableName);

            if (value == null)
            {
                Console.WriteLine("Could not find env variable {0}", environmentVariableName);
                return null;
            }

            Console.WriteLine("Using Amazon connection info from env variable {0}", environmentVariableName);
            return ConnectionInfo.CreateFromString(value);
        }

        protected static ConnectionInfo ConnectionInfoFromFileOrNull(string filePath)
        {
            if (!File.Exists(filePath))
            {
                Console.WriteLine("Could not find file {0}", filePath);
                return null;
            }

            Console.WriteLine("Using Amazon connection info from file {0}", filePath);
            return ConnectionInfo.CreateFromString(File.ReadAllText(filePath));
        }

        protected static ConnectionInfo Throw(string message)
        {
            throw new RebusConfigurationException(message);
        }
    }


    public class AmazonTransportPrefixDecorator : ITransport, ISubscriptionStorage
    {
        private readonly ITransport innerTransport;
        private readonly string prefix;

        public string Address => throw new NotImplementedException();

        public bool IsCentralized => throw new NotImplementedException();

        public AmazonTransportPrefixDecorator(ITransport innerTransport, string prefix)
        {
            this.innerTransport = innerTransport;
            this.prefix = prefix;
        }

        public void CreateQueue(string address)
        {
            this.innerTransport.CreateQueue(this.GetPrefixedAddress(address));
        }

        public Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            return this.innerTransport.Receive(context, cancellationToken);
        }

        public Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            return this.innerTransport.Send(this.GetPrefixedAddress(destinationAddress), message, context);
        }

        public Task<string[]> GetSubscriberAddresses(string topic)
        {
            if (!(this.innerTransport is ISubscriptionStorage subscriptionStorage)) {
                throw new InvalidOperationException("Transport does not support ISubscriptionStorage");
            }

            return subscriptionStorage.GetSubscriberAddresses(this.GetPrefixedAddress(topic));
        }

        public Task RegisterSubscriber(string topic, string subscriberAddress)
        {
            if (!(this.innerTransport is ISubscriptionStorage subscriptionStorage)) {
                throw new InvalidOperationException("Transport does not support ISubscriptionStorage");
            }

            return subscriptionStorage.RegisterSubscriber(this.GetPrefixedAddress(topic), this.GetPrefixedAddress(subscriberAddress));
        }

        public Task UnregisterSubscriber(string topic, string subscriberAddress)
        {
            if (!(this.innerTransport is ISubscriptionStorage subscriptionStorage)) {
                throw new InvalidOperationException("Transport does not support ISubscriptionStorage");
            }

            return subscriptionStorage.UnregisterSubscriber(this.GetPrefixedAddress(topic), this.GetPrefixedAddress(subscriberAddress));
        }

        private string GetPrefixedAddress(string address)
        {
            return $"{this.prefix}{address}";
        }
    }
}
